mod enum_helper;

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
    Generics, Ident, Lit, LitByteStr, Pat, PatLit, Token,
};

/// # Example:
///
/// Get(string, read)
struct Command {
    name: Ident,
    lifetime: Option<Generics>,
    categories: Punctuated<Ident, Token![,]>,
}

// Parse command input
impl Parse for Command {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name: Ident = input.parse()?;
        let lifetime = input.parse().ok();

        let content;
        syn::parenthesized!(content in input);

        let categories = Punctuated::parse_terminated(&content)?;

        Ok(Command {
            name,
            lifetime,
            categories,
        })
    }
}

/// # Example:
///
/// AclCat(admin, dangerous), AclDelUser(admin, dangerous)
struct CommandList {
    commands: Punctuated<Command, Token![,]>,
}

impl Parse for CommandList {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let commands = Punctuated::parse_terminated(input)?;
        Ok(CommandList { commands })
    }
}

#[proc_macro]
pub fn gen_flag(input: TokenStream) -> TokenStream {
    // 初始化分类所含的命令标志，根据每个命令所属的分类，将命令标志加入到对应的分类标志中
    let mut admin_cmds_flag: u128 = 0;
    let mut connection_cmds_flag: u128 = 0;
    let mut read_cmds_flag: u128 = 0;
    let mut write_cmds_flag: u128 = 0;
    let mut keyspace_cmds_flag: u128 = 0;
    let mut string_cmds_flag: u128 = 0;
    let mut list_cmds_flag: u128 = 0;
    let mut hash_cmds_flag: u128 = 0;
    let mut pubsub_cmds_flag: u128 = 0;
    let mut scripting_cmds_flag: u128 = 0;
    let mut dangerous_cmds_flag: u128 = 0;

    // 记录下一个命令标志代表的值
    let mut next_cmd_flag = 1;

    let CommandList { commands } = parse_macro_input!(input as CommandList);

    // 生成<cmd>_CMD_FLAG
    let mut cmd_flag_defs = Vec::new();

    // 为结构体实现CommandFlag trait
    let mut impl_command_flag = Vec::new();

    let mut cmd_name_to_flag_def = Vec::new();

    let mut cmd_flag_to_name_def = Vec::new();

    for command in &commands {
        let name = &command.name;
        let lifetime = &command.lifetime;

        let flag_ident = Ident::new(
            &format!("{}_CMD_FLAG", name.to_string().to_uppercase()),
            name.span(),
        );

        let flag_value = next_cmd_flag;
        next_cmd_flag <<= 1;

        // 将命令标志加入到对应的分类标志中
        let mut cats_flag_def = vec![];
        for category in &command.categories {
            match category.to_string().as_str() {
                "admin" => {
                    admin_cmds_flag |= flag_value;
                    cats_flag_def.push(quote! { ADMIN_CAT_FLAG });
                }
                "connection" => {
                    connection_cmds_flag |= flag_value;
                    cats_flag_def.push(quote! { CONNECTION_CAT_FLAG });
                }
                "read" => {
                    read_cmds_flag |= flag_value;
                    cats_flag_def.push(quote! { READ_CAT_FLAG });
                }
                "write" => {
                    write_cmds_flag |= flag_value;
                    cats_flag_def.push(quote! { WRITE_CAT_FLAG });
                }
                "keyspace" => {
                    keyspace_cmds_flag |= flag_value;
                    cats_flag_def.push(quote! { KEYSPACE_CAT_FLAG });
                }
                "string" => {
                    string_cmds_flag |= flag_value;
                    cats_flag_def.push(quote! { STRING_CAT_FLAG });
                }
                "list" => {
                    list_cmds_flag |= flag_value;
                    cats_flag_def.push(quote! { LIST_CAT_FLAG });
                }
                "hash" => {
                    hash_cmds_flag |= flag_value;
                    cats_flag_def.push(quote! { HASH_CAT_FLAG });
                }
                "pubsub" => {
                    pubsub_cmds_flag |= flag_value;
                    cats_flag_def.push(quote! { PUBSUB_CAT_FLAG });
                }
                "scripting" => {
                    scripting_cmds_flag |= flag_value;
                    cats_flag_def.push(quote! { SCRIPTING_CAT_FLAG });
                }
                "dangerous" => {
                    dangerous_cmds_flag |= flag_value;
                    cats_flag_def.push(quote! { DANGEROUS_CAT_FLAG });
                }
                _ => panic!("Unknown category: {}", category),
            }
        }

        cmd_flag_defs.push(quote! {
            pub const #flag_ident: CmdFlag = #flag_value;
        });

        // 将name转为大写
        let name_uppercase = name.to_string().to_uppercase();
        let name_uppercase_ident = format_ident!("{}", name_uppercase);

        impl_command_flag.push(quote! {
            impl #lifetime CommandFlag for #name #lifetime {
                const NAME: &'static str = stringify!(#name_uppercase_ident);
                const CATS_FLAG: CatFlag = #(#cats_flag_def)|*;
                const CMD_FLAG: CmdFlag = #flag_ident;
            }
        });

        let pat = Pat::Lit(PatLit {
            attrs: Vec::new(),
            lit: Lit::ByteStr(LitByteStr::new(name_uppercase.as_bytes(), name.span())),
        });
        cmd_name_to_flag_def.push(quote! {
            #pat => Some(#flag_ident),
        });

        cmd_flag_to_name_def.push(quote! {
            #flag_value => Some(stringify!(#name_uppercase_ident)),
        });
    }

    // 生成<cat>_CMDS_FLAG
    let cmds_flag_defs = quote! {
        pub const ADMIN_CMDS_FLAG: CmdFlag = #admin_cmds_flag;
        pub const CONNECTION_CMDS_FLAG: CmdFlag = #connection_cmds_flag;
        pub const READ_CMDS_FLAG: CmdFlag = #read_cmds_flag;
        pub const WRITE_CMDS_FLAG: CmdFlag = #write_cmds_flag;
        pub const KEYSPACE_CMDS_FLAG: CmdFlag = #keyspace_cmds_flag;
        pub const STRING_CMDS_FLAG: CmdFlag = #string_cmds_flag;
        pub const LIST_CMDS_FLAG: CmdFlag = #list_cmds_flag;
        pub const HASH_CMDS_FLAG: CmdFlag = #hash_cmds_flag;
        pub const PUBSUB_CMDS_FLAG: CmdFlag = #pubsub_cmds_flag;
        pub const SCRIPTING_CMDS_FLAG: CmdFlag = #scripting_cmds_flag;
        pub const DANGEROUS_CMDS_FLAG: CmdFlag = #dangerous_cmds_flag;
    };

    // 生成<cat>_CAT_FLAG
    let cat_flag_defs = quote! {
        pub const ADMIN_CAT_FLAG: CatFlag = 1;
        pub const CONNECTION_CAT_FLAG: CatFlag = 1 << 1;
        pub const READ_CAT_FLAG: CatFlag = 1 << 2;
        pub const WRITE_CAT_FLAG: CatFlag = 1 << 3;
        pub const KEYSPACE_CAT_FLAG: CatFlag = 1 << 4;
        pub const STRING_CAT_FLAG: CatFlag = 1 << 5;
        pub const LIST_CAT_FLAG: CatFlag = 1 << 6;
        pub const HASH_CAT_FLAG: CatFlag = 1 << 7;
        pub const PUBSUB_CAT_FLAG: CatFlag = 1 << 8;
        pub const SCRIPTING_CAT_FLAG: CatFlag = 1 << 9;
        pub const DANGEROUS_CAT_FLAG: CatFlag = 1 << 10;
    };

    let expanded = quote! {
        pub const ALL_CMDS_FLAG: CmdFlag = #next_cmd_flag - 1;
        pub const NO_CMDS_FLAG: CmdFlag = CmdFlag::MIN | AUTH_CMD_FLAG; // 允许AUTH命令

        #cat_flag_defs
        #cmds_flag_defs
        #(#cmd_flag_defs)*
        #(#impl_command_flag)*
        fn _cmd_name_to_flag(name: &[u8]) -> Option<CmdFlag> {
            match name {
                #(#cmd_name_to_flag_def)*
                _ => None,
            }
        }

        fn _cmd_flag_to_name(flag: CmdFlag) -> Option<&'static str> {
            match flag {
                #(#cmd_flag_to_name_def)*
                _ => None,
            }
        }
    };

    TokenStream::from(expanded)
}

// #[proc_macro_derive(EnumAs, attributes(ignore_enum_as))]
// pub fn derive_enum_as(item: TokenStream) -> TokenStream {
//     let item = parse_macro_input!(item as ItemEnum);
//
//     // 拿到各个variants的ident
//     // fn as_??(&self) -> &T {
//     //
//     // }
//     for v in item.variants {
//         let fun_name = format_ident!("as_{}", v.ident);
//         let return_type =
//         match v.fields {
//
//         }
//     }
//
//     todo!()
// }
