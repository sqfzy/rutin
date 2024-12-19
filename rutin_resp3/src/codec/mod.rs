pub mod decode;
pub mod encode;

#[cfg(test)]
mod test {
    use super::decode::*;
    use crate::codec::encode::Resp3Encoder;
    use bytes::{BufMut, BytesMut};
    use num_bigint::BigInt;
    use std::{
        collections::{HashMap, HashSet},
        io::Cursor,
    };
    use tokio_util::codec::{Decoder, Encoder};

    #[tokio::test]
    async fn codec() {
        type Resp3 = crate::resp3::Resp3<Vec<u8>, String>;

        let mut resp3s_decoded = vec![
            Resp3::SimpleString {
                inner: "OK".to_string(),
                attributes: None,
            },
            Resp3::SimpleError {
                inner: "ERR".to_string(),
                attributes: None,
            },
            Resp3::Integer {
                inner: 42,
                attributes: None,
            },
            Resp3::BlobString {
                inner: "blob data".as_bytes().into(),
                attributes: None,
            },
            Resp3::Array {
                inner: vec![
                    Resp3::Integer {
                        inner: 1,
                        attributes: None,
                    },
                    Resp3::Integer {
                        inner: 2,
                        attributes: None,
                    },
                    Resp3::Integer {
                        inner: 3,
                        attributes: None,
                    },
                ],
                attributes: None,
            },
            Resp3::Null,
            Resp3::Boolean {
                inner: true,
                attributes: None,
            },
            Resp3::Double {
                inner: 3.15,
                attributes: None,
            },
            Resp3::BigNumber {
                inner: BigInt::from(1234567890),
                attributes: None,
            },
            Resp3::BlobError {
                inner: "blob error".as_bytes().into(),
                attributes: None,
            },
            Resp3::VerbatimString {
                format: *b"txt",
                data: "Some string".as_bytes().into(),
                attributes: None,
            },
            Resp3::Map {
                inner: {
                    let mut map = HashMap::new();
                    map.insert(
                        Resp3::SimpleString {
                            inner: "key".to_string(),
                            attributes: None,
                        },
                        Resp3::SimpleString {
                            inner: "value".to_string(),
                            attributes: None,
                        },
                    );
                    map
                },
                attributes: None,
            },
            Resp3::Set {
                inner: {
                    let mut set = HashSet::new();
                    set.insert(Resp3::SimpleString {
                        inner: "element".to_string(),
                        attributes: None,
                    });
                    set
                },
                attributes: None,
            },
            Resp3::Push {
                inner: vec![Resp3::SimpleString {
                    inner: "push".to_string(),
                    attributes: None,
                }],
                attributes: None,
            },
            Resp3::ChunkedString(vec![
                "chunk1".as_bytes().into(),
                "chunk2".as_bytes().into(),
                "chunk3".as_bytes().into(),
            ]),
            Resp3::Hello {
                version: 1,
                auth: Some((b"user".to_vec(), b"password".to_vec())),
            },
        ];

        let resp3s_encoded: Vec<BytesMut> = vec![
            "+OK\r\n",
            "-ERR\r\n",
            ":42\r\n",
            "$9\r\nblob data\r\n",
            "*3\r\n:1\r\n:2\r\n:3\r\n",
            "_\r\n",
            "#t\r\n",
            ",3.15\r\n",
            "(1234567890\r\n",
            "!10\r\nblob error\r\n",
            "=15\r\ntxt:Some string\r\n",
            "%1\r\n+key\r\n+value\r\n",
            "~1\r\n+element\r\n",
            ">1\r\n+push\r\n",
            "$?\r\n;6\r\nchunk1\r\n;6\r\nchunk2\r\n;6\r\nchunk3\r\n;0\r\n",
            "HELLO 1 AUTH user password\r\n",
        ]
        .into_iter()
        .map(|s| s.into())
        .collect();

        let mut encoder = Resp3Encoder;

        for (i, resp3) in resp3s_decoded.iter_mut().enumerate() {
            println!("encoding {:?}", resp3);

            assert_eq!(resp3s_encoded[i], resp3.to_bytes::<BytesMut>());

            let mut dst = BytesMut::new();
            encoder.encode(resp3.clone(), &mut dst).unwrap();
            assert_eq!(resp3s_encoded[i], dst);
        }

        let mut io_read = tokio::io::empty();
        let mut src = resp3s_encoded.iter().fold(BytesMut::new(), |mut acc, b| {
            acc.put_slice(b);
            acc
        });
        let mut src = Cursor::new(&mut src);

        let mut decoder = Resp3Decoder::<Vec<u8>, String>::default();

        for resp3 in resp3s_decoded.iter() {
            println!("try decode {:?}", resp3);

            let decoded = decode_async(&mut io_read, &mut src).await.unwrap().unwrap();
            assert_eq!(resp3, &decoded);

            let decoded = decoder.decode(src.get_mut()).unwrap().unwrap();
            assert_eq!(resp3, &decoded);
        }
    }
}
