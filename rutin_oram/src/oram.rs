use block_padding::{generic_array::{typenum::U4096, ArrayLength}, Block, Iso7816, Padding, Pkcs7};
use bytes::{Bytes, BytesMut};
use indexmap::{IndexMap, map::Entry};
use rand::{Rng, SeedableRng, rngs::StdRng};
use ring::aead::{self, Aad, LessSafeKey, Nonce, UnboundKey};
use ring::rand::{SecureRandom, SystemRandom};
use rutin_resp3::resp3::Resp3;
use rutin_server::{
    cmd::CmdUnparsed,
    error::{RutinError, RutinResult},
    frame::{CheapResp3, StaticResp3},
    server::Connection,
    shared::db::Key,
};
use tokio::net::{TcpListener, TcpStream};

const BLOCK_SIZE: usize = 4096;
type BlockValue = Block<U4096>;
const K: usize = 10;

// 我们不关心MappingKey是什么数据结构，只关心它与Key，Data的映射关系
type MappingKey = u64;

#[derive(Debug, Hash, PartialEq, Eq)]
struct BytesMutWrap(BytesMut);

impl From<&mut [u8]> for BytesMutWrap {
    fn from(value: &mut [u8]) -> Self {
        let value: &[u8] = value;
        Self(value.into())
    }
}

#[derive(Debug)]
pub struct Proxy {
    pub src: Connection<TcpStream>,
    pub dest: Connection<TcpStream>,
    pub map: IndexMap<Key, MappingKey>,
    pub rng: StdRng,
    pub rand: SystemRandom,
    pub skey: LessSafeKey,
}

impl Proxy {
    fn new(src: Connection<TcpStream>, dest: Connection<TcpStream>) -> Self {
        let rand = SystemRandom::new();

        let mut key_bytes = [0; 16];
        rand.fill(&mut key_bytes).unwrap();

        let unbound_key = UnboundKey::new(&aead::AES_128_GCM, &key_bytes).unwrap();
        let skey = LessSafeKey::new(unbound_key);

        Proxy {
            src,
            dest,
            map: IndexMap::new(),
            rng: StdRng::from_entropy(),
            rand,
            skey,
        }
    }

    fn gen_nonce(&self) -> Nonce {
        let mut nonce_bytes = [0u8; 12];
        self.rand.fill(&mut nonce_bytes).unwrap();
        Nonce::assume_unique_for_key(nonce_bytes)
    }

    pub async fn forward_request(self: &mut Proxy, cmd_frame: StaticResp3) -> RutinResult<()> {
        use rutin_server::cmd::CommandFlag;
        use rutin_server::cmd::commands::*;

        // println!("forward_request: {}", cmd_frame);

        let mut cmd = CmdUnparsed::try_from(cmd_frame)?;
        let cmd_name = cmd.cmd_name_uppercase();
        let cmd_name = if let Ok(s) = std::str::from_utf8(cmd_name.as_ref()) {
            s
        } else {
            return Err(RutinError::UnknownCmd);
        };

        #[allow(clippy::let_unit_value)]
        match cmd_name {
            Get::<Bytes>::NAME => {
                let key = cmd.args.front().ok_or(RutinError::Whatever)?;
                let Some(&index) = self.map.get(key) else {
                    // 向client返回nil
                    self.src.write_frame_froce(&StaticResp3::new_null()).await?;
                    return Ok(());
                };

                let mut positions = (0..(K - 1))
                    .map(|_| self.rng.gen_range(0..self.map.len()))
                    .collect::<Vec<_>>();

                let position_len = positions.len();
                positions.push(index as usize);
                let i = self.rng.gen_range(0..position_len);
                positions.swap(i, position_len);

                let mapping_positions = positions
                    .iter()
                    .map(|&i| self.map[i as usize])
                    .collect::<Vec<_>>();

                // read: MGET(mapping_positions)
                let mut mget = vec![b"MGET".to_vec()];
                mget.extend(mapping_positions.iter().map(|p| p.to_string().into_bytes()));
                let mget = Resp3::<Vec<u8>, String>::new_array(
                    mget.into_iter()
                        .map(Resp3::new_blob_string)
                        .collect::<Vec<_>>(),
                );
                self.dest.write_frame_froce(&mget).await?;
                let result = self.dest.read_frame_force().await?;

                let arr = if result.is_array() {
                    result.into_array_unchecked()
                } else {
                    self.src.write_frame_froce(&result).await?;
                    return Ok(());
                };

                let mut path = vec![];
                for (j, blob) in arr.into_iter().enumerate() {
                    let mut ciphertext = blob
                        .into_blob_string()
                        .expect("mappingkey table out-sync with database")
                        .to_vec();

                    let (nonce, ciphervalue_with_tag) = ciphertext.split_at_mut(12);
                    let plainvalue = self
                        .skey
                        .open_in_place(
                            Nonce::assume_unique_for_key(nonce[0..12].try_into().unwrap()),
                            Aad::empty(),
                            ciphervalue_with_tag,
                        )
                        .expect("decryption failure");

                    if j == i {
                        // 向用户返回明文
                        let plainvalue = Iso7816::unpad(BlockValue::from_mut_slice(plainvalue)).unwrap();
                        self.src
                            .write_frame_froce(&CheapResp3::new_blob_string(plainvalue.to_vec()))
                            .await?;
                    }

                    assert!(plainvalue.len() == BLOCK_SIZE);
                    let nonce = self.gen_nonce();
                    let nonce_ = *nonce.as_ref();
                    let tag = self
                        .skey
                        .seal_in_place_separate_tag(nonce, Aad::empty(), plainvalue)
                        .unwrap();
                    let len = ciphertext.len();
                    ciphertext[..12].copy_from_slice(nonce_.as_ref());
                    ciphertext[len - 16..].copy_from_slice(tag.as_ref());

                    path.push(ciphertext);
                }

                // shuffle
                shuffle_vec(&mut positions, &mut path, &mut self.rng);

                // write: MSET(mapping_positions, stash)
                let mut mset = vec![b"MSET".to_vec()];
                for (p, ciphertext) in mapping_positions.iter().zip(path) {
                    let mapping_key = p.to_string().into_bytes();
                    mset.push(mapping_key);
                    mset.push(ciphertext);
                }
                let mget = Resp3::<Vec<u8>, String>::new_array(
                    mset.into_iter()
                        .map(Resp3::new_blob_string)
                        .collect::<Vec<_>>(),
                );
                self.dest.write_frame_froce(&mget).await?;
                self.dest.read_frame_force().await?;

                // sync position_map
                for (i, &pos) in positions.iter().enumerate() {
                    self.map[pos as usize] = mapping_positions[i];
                }
            }
            Set::<Bytes>::NAME => {
                let key = cmd
                    .args
                    .front()
                    .ok_or(RutinError::Whatever)?
                    .to_vec()
                    .into();
                let value = cmd.args.get(1).ok_or(RutinError::Whatever)?;

                let position_len = self.map.len();
                let index = match self.map.entry(key) {
                    Entry::Occupied(e) => *e.get(),
                    Entry::Vacant(e) => {
                        let mapping_key = position_len as MappingKey;
                        e.insert(mapping_key);

                        let pos = value.len();
                        assert!(pos < BLOCK_SIZE);

                        let mut ciphertext = vec![0; 12 + BLOCK_SIZE + 16];
                        ciphertext[12..12 + pos].copy_from_slice(&value);

                        let plaintext =  &mut ciphertext[12..12 + BLOCK_SIZE];
                        let mut plainvalue = BlockValue::from_mut_slice(plaintext);
                        Iso7816::pad(&mut plainvalue, pos);

                        let nonce = self.gen_nonce();
                        let nonce_ = *nonce.as_ref();
                        let tag = self
                            .skey
                            .seal_in_place_separate_tag(nonce, Aad::empty(), &mut plainvalue)
                            .unwrap();
                        let len = ciphertext.len();
                        ciphertext[..12].copy_from_slice(nonce_.as_ref());
                        ciphertext[len - 16..].copy_from_slice(tag.as_ref());

                        let set = Resp3::<Vec<u8>, String>::new_array([
                            Resp3::new_blob_string("SET".to_string()),
                            Resp3::new_blob_string(mapping_key.to_string()),
                            Resp3::new_blob_string(ciphertext.to_vec()),
                        ]);
                        self.dest.write_frame_froce(&set).await?;
                        let result = self.dest.read_frame_force().await?;
                        self.src.write_frame_froce(&result).await?;

                        return Ok(());
                    }
                };

                let mut positions = (0..(K - 1))
                    .map(|_| self.rng.gen_range(0..self.map.len()))
                    .collect::<Vec<_>>();

                let position_len = positions.len();
                positions.push(index as usize);
                let i = self.rng.gen_range(0..position_len);
                positions.swap(i, position_len);

                let mapping_positions = positions
                    .iter()
                    .map(|&i| self.map[i as usize])
                    .collect::<Vec<_>>();

                // read: MGET(mapping_positions)
                let mut mget = vec![b"MGET".to_vec()];
                mget.extend(mapping_positions.iter().map(|p| p.to_string().into_bytes()));
                let mget = Resp3::<Vec<u8>, String>::new_array(
                    mget.into_iter()
                        .map(Resp3::new_blob_string)
                        .collect::<Vec<_>>(),
                );
                self.dest.write_frame_froce(&mget).await?;
                let result = self.dest.read_frame_force().await?;

                let arr = if result.is_array() {
                    result.into_array_unchecked()
                } else {
                    self.src.write_frame_froce(&result).await?;
                    return Ok(());
                };

                let mut path = vec![];
                for (j, blob) in arr.into_iter().enumerate() {
                    let mut ciphertext = blob
                        .into_blob_string()
                        .expect("mappingkey table out-sync with database")
                        .to_vec();

                    let (nonce, ciphervalue_with_tag) = ciphertext.split_at_mut(12);
                    let plainvalue = self
                        .skey
                        .open_in_place(
                            Nonce::assume_unique_for_key(nonce[0..12].try_into().unwrap()),
                            Aad::empty(),
                            ciphervalue_with_tag,
                        )
                        .expect("decryption failure");

                    let plainvalue = if j == i {
                        // 向用户返回明文
                        self.src
                            .write_frame_froce(&CheapResp3::new_blob_string(plainvalue.to_vec()))
                            .await?;
                        &mut value.to_vec()
                    } else {
                        plainvalue
                    };

                    assert!(plainvalue.len() == BLOCK_SIZE);
                    let nonce = self.gen_nonce();
                    let nonce_ = *nonce.as_ref();
                    let tag = self
                        .skey
                        .seal_in_place_separate_tag(nonce, Aad::empty(), plainvalue)
                        .unwrap();
                    let len = ciphertext.len();
                    ciphertext[..12].copy_from_slice(nonce_.as_ref());
                    ciphertext[len - 16..].copy_from_slice(tag.as_ref());

                    path.push(ciphertext);
                }

                // shuffle
                shuffle_vec(&mut positions, &mut path, &mut self.rng);

                // write: MSET(mapping_positions, stash)
                let mut mset = vec![b"MSET".to_vec()];
                for (p, ciphertext) in mapping_positions.iter().zip(path) {
                    let mapping_key = p.to_string().into_bytes();
                    mset.push(mapping_key);
                    mset.push(ciphertext);
                }
                let mget = Resp3::<Vec<u8>, String>::new_array(
                    mset.into_iter()
                        .map(Resp3::new_blob_string)
                        .collect::<Vec<_>>(),
                );
                self.dest.write_frame_froce(&mget).await?;
                self.dest.read_frame_force().await?;

                // sync position_map
                for (i, &pos) in positions.iter().enumerate() {
                    self.map[pos as usize] = mapping_positions[i];
                }
            }
            // 命令中包含子命令
            _ => return Err(RutinError::UnknownCmd),
        }

        Ok(())
    }
}

pub async fn run() {
    let listener = TcpListener::bind("127.0.0.1:6380").await.unwrap();

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let src = Connection::new(stream, usize::MAX);

        let stream = TcpStream::connect("127.0.0.1:6379").await.unwrap();
        let dest = Connection::new(stream, usize::MAX);

        let mut proxy = Proxy::new(src, dest);
        tokio::spawn(async move {
            if let Err(e) = handle(&mut proxy).await {
                eprintln!("Proxy error: {}", e);
            }
        });
    }
}

async fn handle(proxy: &mut Proxy) -> RutinResult<()> {
    loop {
        let res = proxy.src.get_requests().await?;
        if res.is_none() {
            return Ok(());
        }

        let mut start = 0;
        let mut end = 0;
        while let Some(cmd_frame) = proxy.src.requests.pop_front() {
            end += cmd_frame.size();
            if proxy.forward_request(cmd_frame).await.is_err() {
                // 转发失败，直接发送
                proxy
                    .dest
                    .write_all(&proxy.src.reader_buf.get_ref()[start..end])
                    .await
                    .unwrap();
                let resp = proxy.dest.read_frame_force().await.unwrap();
                proxy.src.write_frame_froce(&resp).await.unwrap();
            }
            start = end;
        }
    }
}

fn shuffle_vec<T, U>(vec1: &mut [T], vec2: &mut [U], rng: &mut StdRng) {
    assert!(vec1.len() == vec2.len());

    let n = vec1.len();
    if n <= 1 {
        return; // 长度为 0 或 1 的 Vec 不需要打乱
    }

    // 从 n-1 遍历到 1
    for i in (1..n).rev() {
        // 生成一个在 [0, i] 范围内的随机索引 j
        let j = rng.gen_range(0..=i);

        // 交换 vec[i] 和 vec[j]
        vec1.swap(i, j);
        vec2.swap(i, j);
    }
}
