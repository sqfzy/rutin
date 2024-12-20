use bytes::{Bytes, BytesMut};
use indexmap::IndexMap;
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

const MAX_THRESHOLD: i32 = 1;

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
    // pub encoder: Resp3Encoder,
    // pub read_buffer: BytesMut,
    // pub write_buffer: BytesMut,
    pub map: IndexMap<Key, MappingKey>,
    pub rng: StdRng,
    pub threshold: i32,
    pub rand: SystemRandom,
    pub skey: LessSafeKey,
}

impl Proxy {
    fn new(src: Connection<TcpStream>, dest: Connection<TcpStream>, threshold: i32) -> Self {
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
            threshold,
            rand,
            skey,
        }
    }

    fn reset_threshold(&mut self) {
        self.threshold += MAX_THRESHOLD;
    }

    fn gen_nonce(&self) -> Nonce {
        let mut nonce_bytes = [0u8; 12];
        self.rand.fill(&mut nonce_bytes).unwrap();
        Nonce::assume_unique_for_key(nonce_bytes)
    }

    fn gen_fake_ciphertext(&mut self) -> Vec<u8> {
        let mut ciphertext = Vec::new();

        let nonce = self.gen_nonce();
        ciphertext.extend_from_slice(nonce.as_ref());

        let random_plaintext = (0..self.rng.gen_range(1..=1024))
            .map(|_| self.rng.r#gen())
            .collect::<Vec<u8>>();
        ciphertext.extend_from_slice(random_plaintext.as_ref());

        let tag = self
            .skey
            .seal_in_place_separate_tag(nonce, Aad::empty(), &mut ciphertext)
            .unwrap();
        ciphertext.extend_from_slice(tag.as_ref());

        ciphertext
    }

    pub async fn forward_request(self: &mut Proxy, cmd_frame: StaticResp3) -> RutinResult<()> {
        use rutin_server::cmd::CommandFlag;
        use rutin_server::cmd::commands::*;

        println!("cmd_frame: {}", cmd_frame);

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
                let mut map_len = self.map.len() as u64;
                self.threshold -= 2; // 需要进行两次访问（不包含交换操作）

                // 改变key为mappingkey
                // 转为执行两个getset命令
                let key = cmd.args.front().ok_or(RutinError::Whatever)?;
                let mappingkey = if let Some(mappingkey) = self.map.get(key) {
                    *mappingkey
                } else {
                    self.map.insert(key.as_ref().into(), self.map.len() as u64);
                    // 是新键
                    map_len
                };

                map_len = mappingkey;

                // 第一次getset
                let getset_frame = Resp3::<Vec<u8>, String>::new_array([
                    Resp3::new_blob_string(b"GETSET"),
                    Resp3::new_blob_string(mappingkey.to_string().as_bytes()),
                    Resp3::new_blob_string(self.gen_fake_ciphertext()),
                ]);
                self.dest.write_frame_froce(&getset_frame).await?;

                let resp = self.dest.read_frame_force().await.unwrap();

                if let Some(ciphertext) = resp.into_blob_string() {
                    // 如果键值对存在，则需要将之前的fake ciphertext再替换为真实的ciphertext

                    let mut ciphertext = ciphertext.to_vec();
                    let len = ciphertext.len();
                    // 解密
                    let (nonce, ciphervalue_with_tag) = ciphertext.split_at_mut(12);

                    let plainvalue = self
                        .skey
                        .open_in_place(
                            Nonce::assume_unique_for_key(nonce[0..12].try_into().unwrap()),
                            Aad::empty(),
                            ciphervalue_with_tag,
                        )
                        .unwrap();

                    // 向用户返回明文
                    self.src
                        .write_frame_froce(&CheapResp3::new_blob_string(plainvalue.to_vec()))
                        .await
                        .unwrap();

                    // 重新加密
                    let nonce = self.gen_nonce();
                    let nonce_ = *nonce.as_ref();

                    let tag = self
                        .skey
                        .seal_in_place_separate_tag(nonce, Aad::empty(), plainvalue)
                        .unwrap();
                    ciphertext[..12].copy_from_slice(nonce_.as_ref());
                    ciphertext[len - 16..].copy_from_slice(tag.as_ref());

                    // 先查看是否需要交换。因为我们已经拿到了当前key的value，因此为了尽可能减少
                    // 访问次数，我们先对另一个key执行getset，省去一次对当前key的getset操作
                    // 交换: 随机选择另外一个key，交换它们映射的MappingKey，并且将另一个key的value改为当前key的value
                    if self.threshold <= 0
                        && map_len > 2
                        && let (Some(i), j) = (
                            self.map.get_index_of(key),
                            self.rng.gen_range(0..map_len) as usize,
                        )
                        && i != j
                    {
                        let mappingkey_j = self.map[j];

                        // 将另一个key的value设为当前key的value
                        let getset_frame = StaticResp3::new_array([
                            Resp3::new_blob_string("GETSET".as_bytes()),
                            Resp3::new_blob_string(mappingkey_j.to_string().as_bytes()),
                            Resp3::new_blob_string(ciphertext),
                        ]);
                        self.dest.write_frame_froce(&getset_frame).await?;
                        self.threshold -= 1;
                        let resp = self.dest.read_frame_force().await.unwrap();

                        // 映射表同步交换
                        self.map[i] = mappingkey_j;
                        self.map[j] = mappingkey;

                        // 将ciphertext设为key_j的value
                        // 映射表与键值对一一对应，因此key_j的值一定存在
                        ciphertext = resp.into_blob_string().unwrap().to_vec();

                        self.reset_threshold();
                    }

                    // 第二次getset
                    let get_set_frame = StaticResp3::new_array([
                        Resp3::new_blob_string("GETSET".as_bytes()),
                        Resp3::new_blob_string(mappingkey.to_string().as_bytes()),
                        Resp3::new_blob_string(ciphertext),
                    ]);

                    self.dest.write_frame_froce(&get_set_frame).await?;
                    let _ = self.dest.read_frame_force().await?; // 读取并忽略响应
                } else {
                    // 如果键值对存在，则需要将之前的fake ciphertext移除

                    self.src
                        .write_frame_froce(&StaticResp3::new_null())
                        .await
                        .unwrap();

                    self.dest
                        .write_frame(&CheapResp3::new_array([
                            Resp3::new_blob_string("DEL"),
                            Resp3::new_blob_string(mappingkey.to_string().as_bytes()),
                        ]))
                        .await
                        .unwrap();
                    let _ = self.dest.read_frame_force().await?; // 读取并忽略响应
                }
            }
            Set::<Bytes>::NAME => {
                let mut map_len = self.map.len() as u64;
                self.threshold -= 1; // 需要进行一次访问（不包含交换操作）

                // 改变key为mappingkey
                // 转为执行一个getset命令
                let key = cmd.args.front().ok_or(RutinError::Whatever)?;
                let mappingkey = if let Some(mappingkey) = self.map.get(key) {
                    *mappingkey
                } else {
                    self.map.insert(key.as_ref().into(), self.map.len() as u64);
                    // 是新键
                    map_len
                };

                map_len = mappingkey;

                let value = cmd.args.get(1).ok_or(RutinError::Whatever)?;

                let mut ciphertext = Vec::with_capacity(12 + value.len() + 16);

                let nonce = self.gen_nonce();
                ciphertext.extend_from_slice(nonce.as_ref());
                ciphertext.extend_from_slice(value);
                let tag = self
                    .skey
                    .seal_in_place_separate_tag(nonce, Aad::empty(), &mut ciphertext[12..])
                    .unwrap();
                ciphertext.extend_from_slice(tag.as_ref());

                // 如果键值对存在

                // 先查看是否需要交换。
                // 交换: 随机选择另外一个key，交换它们映射的MappingKey，并且将另一个key的value改为当前key的value
                if self.threshold <= 0
                    && map_len > 2
                    && let (Some(i), j) = (
                        self.map.get_index_of(key),
                        self.rng.gen_range(0..map_len) as usize,
                    )
                    && i != j
                {
                    let mappingkey_j = self.map[j];

                    println!("debug1");

                    // 将另一个key的value设为当前key的value
                    let getset_frame = Resp3::<Vec<u8>, String>::new_array([
                        Resp3::new_blob_string(b"GETSET"),
                        Resp3::new_blob_string(mappingkey_j.to_string().as_bytes()),
                        Resp3::new_blob_string(ciphertext),
                    ]);
                    self.dest.write_frame_froce(&getset_frame).await?;
                    self.threshold -= 1;
                    let resp = self.dest.read_frame_force().await.unwrap();

                    // 映射表同步交换
                    self.map[i] = mappingkey_j;
                    self.map[j] = mappingkey;

                    // 将ciphertext设为key_j的value
                    // 映射表与键值对一一对应，因此key_j的值一定存在
                    ciphertext = resp.into_blob_string().unwrap().to_vec();

                    self.reset_threshold();
                }

                let get_set_frame = Resp3::<Vec<u8>, String>::new_array([
                    Resp3::new_blob_string(b"GETSET"),
                    Resp3::new_blob_string(mappingkey.to_string().as_bytes()),
                    Resp3::new_blob_string(ciphertext),
                ]);

                self.dest.write_frame_froce(&get_set_frame).await?;
                println!("debug2");
                let resp = self.dest.read_frame_force().await?; // 读取并忽略响应
                println!("debug3");

                if resp.is_blob_string() {
                    self.src
                        .write_frame_froce(&CheapResp3::new_simple_string("OK"))
                        .await
                        .unwrap();
                } else {
                    self.src.write_frame_froce(&resp).await.unwrap();
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

        let mut proxy = Proxy::new(src, dest, 1);
        tokio::spawn(async move {
            println!("debug0");
            if let Err(e) = handle(&mut proxy).await {
                eprintln!("Error: {:?}", e);
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
