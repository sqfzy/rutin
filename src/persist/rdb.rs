#![allow(dead_code)]
use crate::{
    conf::RdbConf,
    error::{RutinError, RutinResult},
    server::{NEVER_EXPIRE, UNIX_EPOCH},
    shared::{
        db::{Db, Hash, Key, List, Object, ObjectValue, Set, Str, ZSet},
        Shared,
    },
};
use ahash::{AHashMap, AHashSet};
use bytes::{Buf, BufMut, BytesMut};
use skiplist::OrderedSkipList;
use std::collections::VecDeque;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Mutex,
    time::Duration,
};
use tracing::{info, instrument, trace};

const RDB_VERSION: u32 = 7;

// Opcode
const RDB_OPCODE_AUX: u8 = 0xfa;
const RDB_OPCODE_RESIZEDB: u8 = 0xfb;
const RDB_OPCODE_EXPIRETIME_MS: u8 = 0xfc;
const RDB_OPCODE_EXPIRETIME: u8 = 0xfd;
const RDB_OPCODE_SELECTDB: u8 = 0xfe; // 只允许一个数据库
pub const RDB_OPCODE_EOF: u8 = 0xff;

const RDB_TYPE_STRING: u8 = 0;
const RDB_TYPE_LIST: u8 = 1;
const RDB_TYPE_SET: u8 = 2;
const RDB_TYPE_ZSET: u8 = 3;
const RDB_TYPE_HASH: u8 = 4;
const RDB_TYPE_ZSET_2: u8 = 5;
const RDB_TYPE_MODULE_PRE_GA: u8 = 6;
const RDB_TYPE_MODULE_2: u8 = 7;

const RDB_TYPE_HASH_ZIPMAP: u8 = 9;
const RDB_TYPE_LIST_ZIPLIST: u8 = 10;
const RDB_TYPE_SET_INTSET: u8 = 11;
const RDB_TYPE_ZSET_ZIPLIST: u8 = 12;
const RDB_TYPE_HASH_ZIPLIST: u8 = 13;
const RDB_TYPE_LIST_QUICKLIST: u8 = 14;
const RDB_TYPE_STREAM_LISTPACKS: u8 = 15;
const RDB_TYPE_HASH_LISTPACK: u8 = 16;
const RDB_TYPE_ZSET_LISTPACK: u8 = 17;
const RDB_TYPE_LIST_QUICKLIST_2: u8 = 18;
const RDB_TYPE_STREAM_LISTPACKS_2: u8 = 19;
const RDB_TYPE_SET_LISTPACK: u8 = 20;
const RDB_TYPE_STREAM_LISTPACKS_3: u8 = 21;

// 进行长度编码时，如果开头2bit是11，则后面的数据不是字符串，而是特殊的编码格式
const RDB_ENC_INT8: u8 = 0;
const RDB_ENC_INT16: u8 = 1;
const RDB_ENC_INT32: u8 = 2;
const RDB_ENC_LZF: u8 = 3;

// 保证函数的原子性
static CRITICAL: Mutex<()> = Mutex::const_new(());

#[instrument(level = "info", skip(shared), err)]
pub async fn save_rdb(shared: Shared, rdb_conf: &RdbConf) -> RutinResult<()> {
    let _guard = CRITICAL.lock().await;

    let now = tokio::time::Instant::now();

    let _delay_token = shared.post_office().delay_shutdown_token();

    let RdbConf {
        file_path: path,
        enable_checksum,
        ..
    } = rdb_conf;

    let temp_path = format!("{}.tmp", path);
    let bak_path = format!("{}.bak", path);

    let mut temp_file = tokio::fs::File::create(&temp_path).await?;
    let rdb_data = encode_rdb(shared.db(), *enable_checksum).await;
    temp_file.write_all(&rdb_data).await?;

    if tokio::fs::try_exists(&path).await? {
        tokio::fs::rename(&path, &bak_path).await?;
    }
    tokio::fs::rename(&temp_path, &path).await?;

    info!("save rdb elapsed={:?}", now.elapsed());

    Ok(())
}

#[instrument(level = "info", skip(shared), err)]
pub async fn load_rdb(shared: Shared, rdb_conf: &RdbConf) -> RutinResult<()> {
    let _guard = CRITICAL.lock().await;

    let now = tokio::time::Instant::now();

    let _delay_token = shared.post_office().delay_shutdown_token();

    let RdbConf {
        file_path: path,
        enable_checksum,
        ..
    } = rdb_conf;

    let mut file = tokio::fs::File::open(&path).await?;

    let mut rdb = BytesMut::with_capacity(1024 * 32);
    while file.read_buf(&mut rdb).await? != 0 {}

    decode_rdb(&mut rdb, shared.db(), *enable_checksum).await?;

    info!("load rdb elapsed={:?}", now.elapsed());
    Ok(())
}

#[instrument(level = "debug", skip(db))]
pub async fn encode_rdb(db: &Db, enable_checksum: bool) -> BytesMut {
    let mut buf = BytesMut::with_capacity(1024 * 8);
    buf.extend_from_slice(b"REDIS");
    buf.put_u32(RDB_VERSION);
    buf.put_u8(RDB_OPCODE_SELECTDB);
    buf.put_u32(0);

    for entry in db.entries.iter() {
        let (key, obj) = (entry.key(), entry.value());

        if obj.is_expired() {
            continue;
        }

        if !obj.is_never_expired() {
            encode_timestamp(&mut buf, obj.expire.duration_since(*UNIX_EPOCH));
        }

        match &obj.value {
            ObjectValue::Str(value) => {
                buf.put_u8(RDB_TYPE_STRING);
                encode_key(&mut buf, key);
                encode_str_value(&mut buf, value);
            }
            ObjectValue::List(value) => {
                buf.put_u8(RDB_TYPE_LIST);
                encode_key(&mut buf, key);
                encode_list_value(&mut buf, value);
            }
            ObjectValue::Set(value) => {
                buf.put_u8(RDB_TYPE_SET);
                encode_key(&mut buf, key);
                encode_set_value(&mut buf, value);
            }
            ObjectValue::Hash(value) => {
                buf.put_u8(RDB_TYPE_HASH);
                encode_key(&mut buf, key);
                encode_hash_value(&mut buf, value);
            }
            ObjectValue::ZSet(value) => {
                buf.put_u8(RDB_TYPE_ZSET);
                encode_key(&mut buf, key);
                encode_zset_value(&mut buf, value)
            }
        }
    }

    buf.put_u8(RDB_OPCODE_EOF);
    let checksum = if enable_checksum {
        crc::Crc::<u64>::new(&crc::CRC_64_REDIS).checksum(&buf)
    } else {
        0
    };
    buf.put_u64(checksum);

    buf
}

pub fn encode_timestamp(buf: &mut BytesMut, expire: Duration) {
    buf.put_u8(RDB_OPCODE_EXPIRETIME_MS);
    buf.put_u64_le(expire.as_millis() as u64);
}

pub fn encode_zset_value(buf: &mut BytesMut, value: &ZSet) {
    match value {
        ZSet::SkipList(zset) => {
            encode_length(buf, zset.len() as u32, None);

            let mut buf2 = itoa::Buffer::new();
            for elem in &**zset {
                encode_raw(buf, elem.member().as_bytes(&mut buf2));
                encode_raw(buf, ryu::Buffer::new().format(elem.score()).as_bytes());
            }
        }
        ZSet::ZipSet => unimplemented!(),
    }
}

pub fn encode_hash_value(buf: &mut BytesMut, value: &Hash) {
    match value {
        Hash::HashMap(hash) => {
            encode_length(buf, hash.len() as u32, None);

            let mut buf2 = itoa::Buffer::new();
            for (k, v) in &**hash {
                encode_raw(buf, k);
                encode_raw(buf, v.as_bytes(&mut buf2));
            }
        }
        Hash::ZipList => unimplemented!(),
    }
}

pub fn encode_set_value(buf: &mut BytesMut, value: &Set) {
    match value {
        Set::HashSet(set) => {
            encode_length(buf, set.len() as u32, None);

            let mut buf2 = itoa::Buffer::new();
            for elem in &**set {
                encode_raw(buf, elem.as_bytes(&mut buf2));
            }
        }
        Set::IntSet => unimplemented!(),
    }
}

pub fn encode_list_value(buf: &mut BytesMut, value: &List) {
    match value {
        List::LinkedList(list) => {
            encode_length(buf, list.len() as u32, None);
            let mut buf2 = itoa::Buffer::new();
            for elem in list {
                encode_raw(buf, elem.as_bytes(&mut buf2));
            }
        }
        List::ZipList => unimplemented!(),
    }
}

pub fn encode_str_value(buf: &mut BytesMut, value: &Str) {
    match value {
        Str::Int(i) => match (*i).try_into() {
            Ok(i) => encode_int(buf, i),
            Err(_) => encode_raw(buf, itoa::Buffer::new().format(*i).as_bytes()),
        },
        Str::Raw(ref s) => encode_raw(buf, s),
    }
}

pub fn encode_raw(buf: &mut BytesMut, value: &[u8]) {
    encode_length(buf, value.len() as u32, None);
    buf.put_slice(value);
}

pub fn encode_int(buf: &mut BytesMut, value: i32) {
    if value >= i8::MIN as i32 && value <= i8::MAX as i32 {
        encode_length(buf, 0, Some(RDB_ENC_INT8));
        buf.put_i8(value as i8);
    } else if value >= i16::MIN as i32 && value <= i16::MAX as i32 {
        encode_length(buf, 0, Some(RDB_ENC_INT16));
        buf.put_i16(value as i16);
    } else {
        encode_length(buf, 0, Some(RDB_ENC_INT32));
        buf.put_i32(value);
    }
}

pub fn encode_key(buf: &mut BytesMut, key: &[u8]) {
    encode_length(buf, key.len() as u32, None);
    buf.put_slice(key);
}

pub fn encode_length(buf: &mut BytesMut, len: u32, special_format: Option<u8>) {
    if let Some(special_format) = special_format {
        // 11000000
        buf.put_u8(0xc0 | special_format);
        return;
    }
    if len < 1 << 6 {
        // 00xxxxxx
        buf.put_u8(len as u8);
    } else if len < 1 << 14 {
        // 01xxxxxx(高6位) xxxxxxxx(低8位)
        buf.put_u8((len >> 8 | 0x40) as u8);
        buf.put_u8(len as u8);
    } else {
        // 10xxxxxx(丢弃) xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
        buf.put_u8(0x80);
        buf.put_u32(len);
    }
}

#[instrument(level = "debug", skip(rdb_data, db), err)]
pub async fn decode_rdb(
    rdb_data: &mut BytesMut,
    db: &Db,
    enable_checksum: bool,
) -> RutinResult<()> {
    if enable_checksum {
        let mut buf = [0; 8];
        buf.copy_from_slice(&rdb_data[rdb_data.len() - 8..]);
        let expected_checksum = u64::from_be_bytes(buf);

        let crc = crc::Crc::<u64>::new(&crc::CRC_64_REDIS);
        let checksum = crc.checksum(&rdb_data[..rdb_data.len() - 8]);

        if expected_checksum != checksum {
            return Err(RutinError::from(format!(
                "checksum failed, expected: {:?}, got: {:?}",
                expected_checksum, checksum
            )));
        }
    }

    let magic = rdb_data.split_to(5);
    if magic != b"REDIS"[..] {
        return Err(RutinError::from(
            "magic string should be REDIS, but got {magic:?}",
        ));
    }
    let _rdb_version = rdb_data.get_u32();

    let mut expire = *NEVER_EXPIRE;
    loop {
        match rdb_data.get_u8() {
            RDB_OPCODE_EOF => {
                trace!("EOF");
                // 丢弃EOF后面的checksum
                rdb_data.advance(8);
                break;
            }
            RDB_OPCODE_SELECTDB => {
                let _db_num = decode_length(rdb_data)?;
                continue;
            }
            RDB_OPCODE_RESIZEDB => {
                let _db_size = decode_length(rdb_data)?;
                let _expires_size = decode_length(rdb_data)?;

                trace!(
                    "Resizedb: db_size: {:?}, expires_size: {:?}",
                    _db_size,
                    _expires_size
                );
                continue;
            }
            RDB_OPCODE_AUX => {
                let _key = decode_key(rdb_data)?;
                let _value = decode_str_value(rdb_data)?;

                trace!("Auxiliary fields: key: {:?}, value: {:?}", _key, _value);
                continue;
            }
            RDB_OPCODE_EXPIRETIME_MS => {
                let ms = rdb_data.get_u64_le();
                expire = *UNIX_EPOCH + Duration::from_millis(ms);

                trace!("Expiretime_ms: {:?}", expire);
            }
            RDB_OPCODE_EXPIRETIME => {
                let sec = rdb_data.get_u32_le();
                expire = *UNIX_EPOCH + Duration::from_secs(sec as u64);
                trace!("Expiretime: {:?}", expire);
            }
            RDB_TYPE_STRING => {
                let key = decode_key(rdb_data)?;
                let value = decode_str_value(rdb_data)?;

                trace!("String: key: {:?}, value: {:?}", key, value);

                db.insert_object(&key, Object::with_expire(value, expire))
                    .await?;
                expire = *NEVER_EXPIRE;
            }
            RDB_TYPE_LIST => {
                let key = decode_key(rdb_data)?;
                let value = decode_list_kv(rdb_data)?;

                trace!("List: key: {:?}, value: {:?}", key, value);

                db.insert_object(&key, Object::with_expire(value, expire))
                    .await?;
                expire = *NEVER_EXPIRE;
            }
            RDB_TYPE_HASH => {
                let key = decode_key(rdb_data)?;
                let value = decode_hash_value(rdb_data)?;

                trace!("Hash: key: {:?}, value: {:?}", key, value);

                db.insert_object(&key, Object::with_expire(value, expire))
                    .await?;
                expire = *NEVER_EXPIRE;
            }
            RDB_TYPE_SET => {
                let key = decode_key(rdb_data)?;
                let value = decode_set_value(rdb_data)?;

                trace!("Set: key: {:?}, value: {:?}", key, value);

                db.insert_object(&key, Object::with_expire(value, expire))
                    .await?;
                expire = *NEVER_EXPIRE;
            }
            RDB_TYPE_ZSET => {
                let key = decode_key(rdb_data)?;
                let value = decode_zset_value(rdb_data)?;

                trace!("ZSet: key: {:?}, value: {:?}", key, value);

                db.insert_object(&key, Object::with_expire(value, expire))
                    .await?;
                expire = *NEVER_EXPIRE;
            }
            invalid_ctrl => {
                return Err(RutinError::from(format!(
                    "invalid RDB control byte: {:?}",
                    invalid_ctrl
                )))
            }
        }
    }

    Ok(())
}

pub fn decode_zset_value(bytes: &mut BytesMut) -> RutinResult<ZSet> {
    if let Length::Len(zset_size) = decode_length(bytes)? {
        let mut zset = OrderedSkipList::new();
        for _ in 0..zset_size {
            let member = decode_str_value(bytes)?.to_bytes();
            let score = std::str::from_utf8(&decode_str_value(bytes)?.to_bytes())?
                .parse()
                .map_err(|_| RutinError::from("invalid zset score"))?;

            zset.insert((score, member).into());
        }
        Ok(ZSet::SkipList(Box::new(zset)))
    } else {
        Err(RutinError::from("invalid zset length"))
    }
}

pub fn decode_set_value(bytes: &mut BytesMut) -> RutinResult<Set> {
    if let Length::Len(set_size) = decode_length(bytes)? {
        let mut set = AHashSet::with_capacity(set_size);
        for _ in 0..set_size {
            let elem = decode_str_value(bytes)?.to_bytes();
            set.insert(elem.into());
        }

        Ok(Set::HashSet(Box::new(set)))
    } else {
        Err(RutinError::from("invalid set length"))
    }
}

pub fn decode_hash_value(bytes: &mut BytesMut) -> RutinResult<Hash> {
    if let Length::Len(hash_size) = decode_length(bytes)? {
        let mut hash = AHashMap::with_capacity(hash_size);
        for _ in 0..hash_size {
            let field = decode_key(bytes)?;
            let value = decode_str_value(bytes)?.to_bytes();
            hash.insert(field, value.into());
        }

        Ok(Hash::HashMap(Box::new(hash)))
    } else {
        Err(RutinError::from("invalid hash length"))
    }
}

pub fn decode_list_kv(bytes: &mut BytesMut) -> RutinResult<List> {
    if let Length::Len(list_size) = decode_length(bytes)? {
        let mut list = VecDeque::with_capacity(list_size);
        for _ in 0..list_size {
            let elem = decode_str_value(bytes)?.to_bytes();
            list.push_back(elem.into());
        }

        Ok(List::LinkedList(list))
    } else {
        Err(RutinError::from("invalid list length"))
    }
}

pub fn decode_str_value(bytes: &mut BytesMut) -> RutinResult<Str> {
    let str = match decode_length(bytes)? {
        Length::Len(len) => Str::Raw(bytes.split_to(len).freeze()),
        Length::Int8 => Str::from(bytes.get_i8() as i128),
        Length::Int16 => Str::from(bytes.get_i16() as i128),
        Length::Int32 => Str::from(bytes.get_i32() as i128),
        Length::Lzf => {
            if let (Length::Len(compressed_len), Length::Len(_uncompressed_len)) =
                (decode_length(bytes)?, decode_length(bytes)?)
            {
                let raw = lzf::lzf_decompress(&bytes.split_to(compressed_len));
                Str::Raw(raw)
            } else {
                return Err(RutinError::from("invalid LZF length"));
            }
        }
    };

    Ok(str)
}

pub fn decode_key(bytes: &mut BytesMut) -> RutinResult<Key> {
    if let Length::Len(len) = decode_length(bytes)? {
        Ok(bytes.split_to(len).freeze().into())
    } else {
        Err(RutinError::from("invalid key length"))
    }
}

#[derive(Debug)]
pub enum Length {
    Len(usize),
    Int8,
    Int16,
    Int32,
    Lzf,
}

pub fn decode_length(bytes: &mut BytesMut) -> RutinResult<Length> {
    let ctrl = bytes.get_u8();
    let len = match ctrl >> 6 {
        // 00
        0 => Length::Len(ctrl as usize),
        // 01
        1 => {
            let mut res = 0_usize;
            res |= ((ctrl & 0x3f) as usize) << 8; // ctrl & 0011 1111
            res |= (bytes.get_u8()) as usize;
            Length::Len(res)
        }
        // 10
        2 => Length::Len(<u32>::from_be_bytes([
            bytes.get_u8(),
            bytes.get_u8(),
            bytes.get_u8(),
            bytes.get_u8(),
        ]) as usize),
        // 11
        3 => match ctrl & 0x3f {
            0 => Length::Int8,
            1 => Length::Int16,
            2 => Length::Int32,
            3 => Length::Lzf,
            _ => return Err(RutinError::from("invalid length encoding")),
        },
        _ => return Err(RutinError::from("invalid length encoding")),
    };

    Ok(len)
}

mod lzf {
    #![allow(dead_code)]
    use bytes::{BufMut, Bytes, BytesMut};
    use std::collections::HashMap;

    /// LZF压缩算法实现
    const WINDOW_SIZE: usize = 3; // 滑动窗口大小
    const MAX_LIT: usize = 1 << 5; // 最大字面量长度=32
    const MAX_OFF: usize = 1 << 13; //  最大偏移量。off <= 0001 1111 1111 1111, 高三位用来存放长度
    const MAX_REF: usize = (1 << 8) + (1 << 3); // 最大引用长度=264

    pub fn lzf_compress(input: &[u8]) -> Bytes {
        let mut output = BytesMut::with_capacity(input.len());
        // hash_table键：滑动窗口中的字节序列，值：滑动窗口中首个字节的索引
        let mut hash_table: HashMap<&[u8], usize> = HashMap::with_capacity(input.len());
        // 'iidx'是输入数组的索引指针。它跟踪当前正在处理的输入字节的位置。从0开始，随着算法逐字节（或在找到匹配时跳过更多）处理输入数据而增加。
        let mut iidx = 0;
        // 'lit'用于跟踪当前未匹配字面量序列的长度。
        let mut lit: usize = 0;

        // NOTE: 以"aabcdeabcdf"，滑动窗口大小为3为例
        // 循环轮次:    第一次循环   第二次循环   第三次循环   第四次循环   第五次循环   第六次循环   第七次循环
        // hash_table:  aab->0       abc->1       bcd->2       cde->3       dea->4       eab->5       abc->6(发生碰撞)
        // iidx:        0            1-碰撞后->6  2            3            4            5            6
        // lit:         0            1            2            3            4            5            6
        // ref:         0            1            2            3            4            5            6

        // NOTE:
        // 循环轮次:    第八次循环
        // 滑动窗口:    f..
        // 索引范围:    10-12(超出范围)
        // iidx:        10
        // lit:         0
        // ref:

        while let Some(slid_wnd) = input.get(iidx..iidx + WINDOW_SIZE) {
            let reference = hash_table.get(slid_wnd).cloned();
            // 不管当前字节序列是否导致碰撞（滑动窗口中的字节序列曾经出现过），都将当前字节序列的索引更新到hash_table中，
            // 也就是说，hash_table中的值总是字节序列最新出现的位置
            hash_table.insert(slid_wnd, iidx);

            if let Some(reference) = reference {
                // NOTE: 当滑动窗口为"abc"时发生碰撞，lit=6，iidx=6; off=4 len=3

                // println!("iidx: {}, reference: {}", iidx, reference);
                let off = iidx - reference - 1;
                // 若偏移量大于最大偏移量，则当作字面量处理
                if off > MAX_OFF {
                    lit += 1;
                    iidx += 1;

                    // 字面量长度不能到MAX_LIT
                    if lit >= MAX_LIT - 1 {
                        output.put_u8(lit as u8 - 1); // 将字面量长度写入输出（由于编码规则，长度需要减1）
                        output.extend(&input[iidx - lit..iidx]); // 将字面量数据本身写入输出缓冲区
                        lit = 0;
                    }
                    continue;
                }

                let mut len = WINDOW_SIZE;

                // NOTE: 还可以匹配一个字符("d")；len=4
                //
                // 继续匹配直到匹配长度达到最大值或者匹配失败
                while let (Some(ch), true) = (input.get(iidx + len), len <= MAX_REF) {
                    if ch != &input[reference + len] {
                        break;
                    }
                    len += 1;
                }

                if lit > 0 {
                    output.put_u8(lit as u8 - 1); // 将字面量长度写入输出（由于编码规则，长度需要减1）
                    output.extend(&input[iidx - lit..iidx]); // 将字面量数据本身写入输出缓冲区
                    lit = 0;
                }

                // NOTE: repeat_len=2表示重复序列长度为WINDOW_SIZE+1。
                //
                // 解压缩时会自动加上WINDOW_SIZE，且由于解压缩时需要通过控制字符的值的大小
                // 区分字面量和重复序列，因此len不能为0。即len=2时，表示WINDOW_SIZE+1个字节的字面量，
                // len=3时，表示WINDOW_SIZE+2个字节的字面量，以此类推。
                let repeat_len = len - WINDOW_SIZE + 1;
                if repeat_len < 7 {
                    // NOTE: off=0000 0000 0000 0100,取高8位 repeat_len= 0000 0000 0000 0 001取低三位
                    // output.put_u8(0010 0000 + 0000 0000)
                    //
                    // 长度小于7则用一个字节表示偏移量和重复长度
                    // 取off的高8位(off<=MAX_OFF因此高三位总是为0)，然后将repeat_len的低3位放到off的高3位
                    output.put_u8(((off >> 8) + (repeat_len << 5)) as u8);
                } else {
                    // TEST: println!("off={off}");

                    // 长度大于等于7则用两个字节表示偏移量和重复长度
                    output.put_u8(((off >> 8) + (7 << 5)) as u8);
                    output.put_u8(repeat_len as u8 - 7);
                }
                // 长度(3bit)，偏移量(5bit)，偏移量(8bit)或者长度(3bit)，偏移量(5bit)，长度(8bit)，偏移量(8bit)
                output.put_u8(off as u8);
                // NOTE: 最终输出0010 0000 0000 0000 0000 0100

                // NOTE: 滑动窗口需要向前划过bcd,cdf,df., 超出了input的范围。跳出循环, 进行边界处理
                //
                // 让wnd向前滑动len个字节并更新hash_table，如果wnd会超出input的范围
                // 则跳出循环，进行边界处理
                let new_iidx = iidx + len;
                if new_iidx + WINDOW_SIZE > input.len() {
                    iidx = new_iidx;
                    break;
                }
                iidx += 1; // 当前wnd中的字节序列已经存入hash_table中了，但iidx一直未更新，因此wnd需要向前滑动1位
                for i in iidx..new_iidx {
                    hash_table.insert(&input[i..(WINDOW_SIZE + i)], i);
                }
                iidx = new_iidx;
            } else {
                lit += 1;
                iidx += 1;

                // 如果字面量长度不能到MAX_LIT
                if lit >= MAX_LIT - 1 {
                    output.put_u8(lit as u8 - 1); // 将字面量长度写入输出（由于编码规则，长度需要减1）
                    output.extend(&input[iidx - lit..iidx]); // 将字面量数据本身写入输出缓冲区
                    lit = 0;
                }
            }
        }

        lit += input.len() - iidx; // 计算剩余的字面量长度
        if lit > 0 {
            // 编码长度时减1是一种常见的技巧，用于优化编码空间。例如，如果直接编码长度，那么长度为0的情况会占用一个有效的编码空间，
            // 但在实际中，长度为0是不会发生的（因为至少会有1个字节的字面量）。通过减去1，允许我们在相同的编码空间内表示更大的长度值。
            output.put_u8(lit as u8 - 1); // 将字面量长度写入输出（由于编码规则，长度需要减1）
            output.extend(&input[input.len() - lit..input.len()]); // 将字面量数据本身写入输出缓冲区
        }

        output.freeze()
    }

    pub fn lzf_decompress(input: &[u8]) -> Bytes {
        // NOTE: input: 5 a a b c d e (4,4) 0 f
        let mut output = BytesMut::with_capacity(input.len() * 2);
        let mut iidx = 0;
        while iidx < input.len() {
            let ctrl = input[iidx]; // 读取控制字节
                                    // iidx += 1; // 指向第二个控制字节

            // 控制字节小于等于32，表示这是一个字面量序列
            if ctrl < MAX_LIT as u8 {
                // 字面量长度(1B) + 字面量数据
                // NOTE: len=6; output放入aabcde, output_len=6; iidx=7
                let len = ctrl as usize + 1; // 读取字面量长度
                                             // TEST: println!("iidx={iidx}, len={}", len);
                iidx += 1;
                output.extend(&input[iidx..iidx + len]);
                iidx += len; // 指向下一个控制字节
            } else {
                // 否则表示这是一个重复序列。长度和偏移量(2B或3B)

                // NOTE: 0010 0000 0000 0000 0000 0100
                // len=2+WINDOW_SIZE-1=4, off=4, output_len=6
                let mut len: usize = ctrl as usize >> 5; // 高3位表示重复序列长度
                len = len + WINDOW_SIZE - 1;
                let mut off = (ctrl as usize & 0x1f) << 8; // 低5位表示偏移量的高位
                                                           // 如果len=7+WINDOW_SIZE-1，表示需要再读取一个字节来获取len
                if len == 7 + WINDOW_SIZE - 1 {
                    iidx += 1; // 指向第二个控制字节
                    len += input[iidx] as usize;
                }
                iidx += 1; // 指向第二个或第三个控制字节
                off += input[iidx] as usize;
                // TEST: println!("out_put={}, off={}, len={}", output.len(), off, len);
                let refrence = output.len() - off - 1;

                // NOTE: len=4, off=4, output中有aabcde; reference=1; 向output写入abcd
                for i in 0..len {
                    output.put_u8(output[refrence + i]);
                }

                iidx += 1; // 指向下一个控制字节
            }
        }

        output.freeze()
    }

    #[test]
    fn test_lzf() {
        use rand::Rng;

        for _ in 0..100 {
            let mut rnd = rand::thread_rng();
            // let len = rnd.gen_range(0..=255);
            let len = 10000;
            let mut input = Vec::with_capacity(len);
            for _ in 0..len {
                input.push(rnd.gen_range(0..=255));
            }

            let compressed = lzf_compress(input.as_slice());
            let compressibility = compressed.len() as f64 / len as f64;
            tracing::info!(
                "压缩前的数据长度: {}, 压缩后的数据长度: {}, 压缩率: {:.2}%",
                len,
                compressed.len(),
                compressibility * 100.0
            );
            let decompressed = lzf_decompress(&compressed);
            // assert_eq!(Bytes::from_static(input), decompressed);
            assert_eq!(Bytes::from(input), decompressed);

            // }
        }
    }
}

#[cfg(test)]
mod rdb_test {
    use super::*;
    use crate::{
        shared::db::Object,
        util::{gen_test_shared, test_init},
    };
    use bytes::BytesMut;
    use tokio::time::Instant;

    #[test]
    fn rdb_length_codec_test() {
        test_init();

        let mut buf = BytesMut::with_capacity(1024);
        encode_length(&mut buf, 0, None);
        assert_eq!(buf.as_ref(), [0]);
        let len = decode_length(&mut buf).unwrap();
        if let Length::Len(len) = len {
            assert_eq!(len, 0);
        } else {
            panic!("decode length failed");
        }
        buf.clear();

        encode_length(&mut buf, 63, None);
        assert_eq!(buf.as_ref(), [63]);
        let len = decode_length(&mut buf).unwrap();
        if let Length::Len(len) = len {
            assert_eq!(len, 63);
        } else {
            panic!("decode length failed");
        }
        buf.clear();

        encode_length(&mut buf, 64, None);
        assert_eq!(buf.as_ref(), [0x40, 64]);
        let len = decode_length(&mut buf).unwrap();
        if let Length::Len(len) = len {
            assert_eq!(len, 64);
        } else {
            panic!("decode length failed");
        }
        buf.clear();

        encode_length(&mut buf, 16383, None);
        assert_eq!(buf.as_ref(), [0x7f, 0xff]);
        let len = decode_length(&mut buf).unwrap();
        if let Length::Len(len) = len {
            assert_eq!(len, 16383);
        } else {
            panic!("decode length failed");
        }
        buf.clear();

        encode_length(&mut buf, 16384, None);
        assert_eq!(buf.as_ref(), [0x80, 0, 0, 0x40, 0]);
        let len = decode_length(&mut buf).unwrap();
        if let Length::Len(len) = len {
            assert_eq!(len, 16384);
        } else {
            panic!("decode length failed");
        }
        buf.clear();
    }

    #[test]
    fn rdb_key_codec_test() {
        test_init();

        let mut buf = BytesMut::with_capacity(1024);
        encode_key(&mut buf, "key".as_bytes());
        assert_eq!(buf.as_ref(), [3, 107, 101, 121]);
        let key = decode_key(&mut buf).unwrap();
        assert_eq!(key, "key".as_bytes());
        buf.clear();
    }

    #[tokio::test]
    async fn rdb_save_and_load_test() {
        test_init();

        let shared = gen_test_shared();
        let db = shared.db();

        let str1 = Object::with_expire(Str::from("hello"), *NEVER_EXPIRE);
        let str2 = Object::with_expire(Str::from("10"), *NEVER_EXPIRE);
        let str3 = Object::with_expire(Str::from("200"), Instant::now() + Duration::from_secs(10));
        let str4 =
            Object::with_expire(Str::from("hello"), Instant::now() + Duration::from_secs(10));

        db.insert_object(&Key::from("str1"), str1.clone())
            .await
            .unwrap();
        db.insert_object(&Key::from("str2"), str2.clone())
            .await
            .unwrap();
        db.insert_object(&Key::from("str3"), str3.clone())
            .await
            .unwrap();
        db.insert_object(&Key::from("str4"), str4.clone())
            .await
            .unwrap();

        let l1 = Object::with_expire(List::default(), *NEVER_EXPIRE);
        let l2 = Object::with_expire(List::from(["v1".into(), "v2".into()]), *NEVER_EXPIRE);
        let l3 = Object::with_expire(List::default(), Instant::now() + Duration::from_secs(10));
        let l4 = Object::with_expire(
            List::from(["v1".into(), "v2".into()]),
            Instant::now() + Duration::from_secs(10),
        );

        db.insert_object(&Key::from("l1"), l1.clone())
            .await
            .unwrap();
        db.insert_object(&Key::from("l2"), l2.clone())
            .await
            .unwrap();
        db.insert_object(&Key::from("l3"), l3.clone())
            .await
            .unwrap();
        db.insert_object(&Key::from("l4"), l4.clone())
            .await
            .unwrap();

        let s1 = Object::with_expire(Set::default(), *NEVER_EXPIRE);
        let s2 = Object::with_expire(Set::from(["v1".into(), "v2".into()]), *NEVER_EXPIRE);
        let s3 = Object::with_expire(Set::default(), Instant::now() + Duration::from_secs(10));
        let s4 = Object::with_expire(
            Set::from(["v1".into(), "v2".into()]),
            Instant::now() + Duration::from_secs(10),
        );

        db.insert_object(&Key::from("s1"), s1.clone())
            .await
            .unwrap();
        db.insert_object(&Key::from("s2"), s2.clone())
            .await
            .unwrap();
        db.insert_object(&Key::from("s3"), s3.clone())
            .await
            .unwrap();
        db.insert_object(&Key::from("s4"), s4.clone())
            .await
            .unwrap();

        let h1 = Object::with_expire(Hash::default(), *NEVER_EXPIRE);
        let h2 = Object::with_expire(
            Hash::from([("f1".into(), "v1".into()), ("f2".into(), "v2".into())]),
            *NEVER_EXPIRE,
        );
        let h3 = Object::with_expire(Hash::default(), Instant::now() + Duration::from_secs(10));
        let h4 = Object::with_expire(
            Hash::from([("f1".into(), "v1".into()), ("f2".into(), "v2".into())]),
            Instant::now() + Duration::from_secs(10),
        );

        db.insert_object(&Key::from("h1"), h1.clone())
            .await
            .unwrap();
        db.insert_object(&Key::from("h2"), h2.clone())
            .await
            .unwrap();
        db.insert_object(&Key::from("h3"), h3.clone())
            .await
            .unwrap();
        db.insert_object(&Key::from("h4"), h4.clone())
            .await
            .unwrap();

        let zs1 = Object::with_expire(ZSet::default(), *NEVER_EXPIRE);
        let zs2 = Object::with_expire(ZSet::from([(1_f64, "v1"), (2_f64, "v2")]), *NEVER_EXPIRE);
        let zs3 = Object::with_expire(ZSet::default(), Instant::now() + Duration::from_secs(10));
        let zs4 = Object::with_expire(
            ZSet::from([(1_f64, "v1"), (2_f64, "v2")]),
            Instant::now() + Duration::from_secs(10),
        );

        db.insert_object(&Key::from("zs1"), zs1.clone())
            .await
            .unwrap();
        db.insert_object(&Key::from("zs2"), zs2.clone())
            .await
            .unwrap();
        db.insert_object(&Key::from("zs3"), zs3.clone())
            .await
            .unwrap();
        db.insert_object(&Key::from("zs4"), zs4.clone())
            .await
            .unwrap();

        save_rdb(
            shared,
            &RdbConf {
                file_path: "tests/rdb/rdb_test.rdb".to_string(),
                save: None,
                enable_checksum: true,
            },
        )
        .await
        .unwrap();

        let shared = gen_test_shared();
        load_rdb(
            shared,
            &RdbConf {
                file_path: "tests/rdb/rdb_test.rdb".to_string(),
                save: None,
                enable_checksum: true,
            },
        )
        .await
        .unwrap();

        assert_eq!(
            db.get_object("str1".as_bytes()).await.unwrap().value(),
            &str1
        );
        assert_eq!(
            db.get_object("str2".as_bytes()).await.unwrap().value(),
            &str2
        );
        assert_eq!(
            db.get_object("str3".as_bytes()).await.unwrap().value(),
            &str3
        );
        assert_eq!(
            db.get_object("str4".as_bytes()).await.unwrap().value(),
            &str4
        );

        assert_eq!(db.get_object("l1".as_bytes()).await.unwrap().value(), &l1);
        assert_eq!(db.get_object("l2".as_bytes()).await.unwrap().value(), &l2);
        assert_eq!(db.get_object("l3".as_bytes()).await.unwrap().value(), &l3);
        assert_eq!(db.get_object("l4".as_bytes()).await.unwrap().value(), &l4);

        assert_eq!(db.get_object("s1".as_bytes()).await.unwrap().value(), &s1);
        assert_eq!(db.get_object("s2".as_bytes()).await.unwrap().value(), &s2);
        assert_eq!(db.get_object("s3".as_bytes()).await.unwrap().value(), &s3);
        assert_eq!(db.get_object("s4".as_bytes()).await.unwrap().value(), &s4);

        assert_eq!(db.get_object("h1".as_bytes()).await.unwrap().value(), &h1);
        assert_eq!(db.get_object("h2".as_bytes()).await.unwrap().value(), &h2);
        assert_eq!(db.get_object("h3".as_bytes()).await.unwrap().value(), &h3);
        assert_eq!(db.get_object("h4".as_bytes()).await.unwrap().value(), &h4);

        assert_eq!(db.get_object("zs1".as_bytes()).await.unwrap().value(), &zs1);
        assert_eq!(db.get_object("zs2".as_bytes()).await.unwrap().value(), &zs2);
        assert_eq!(db.get_object("zs3".as_bytes()).await.unwrap().value(), &zs3);
        assert_eq!(db.get_object("zs4".as_bytes()).await.unwrap().value(), &zs4);
    }
}
