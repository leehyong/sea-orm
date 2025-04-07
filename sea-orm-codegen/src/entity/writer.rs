use crate::{util::escape_rust_keyword, ActiveEnum, Entity};
use heck::{ToSnakeCase, ToUpperCamelCase};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use std::collections::HashMap;
use std::ptr::replace;
use std::{collections::BTreeMap, str::FromStr};
use syn::{punctuated::Punctuated, token::Comma};
use tracing::info;

#[derive(Clone, Debug)]
pub struct EntityWriter {
    pub(crate) entities: Vec<Entity>,
    pub(crate) enums: BTreeMap<String, ActiveEnum>,
}

pub struct WriterOutput {
    pub files: Vec<OutputFile>,
}

pub struct OutputFile {
    pub name: String,
    pub content: String,
}

#[derive(Default, PartialEq, Eq, Clone, Copy, Debug)]
pub enum WithPrelude {
    #[default]
    All,
    None,
    AllAllowUnusedImports,
}

#[derive(PartialEq, Eq, Debug)]
pub enum WithSerde {
    None,
    Serialize,
    Deserialize,
    Both,
}

#[derive(Debug)]
pub enum DateTimeCrate {
    Chrono,
    Time,
}

#[derive(Debug)]
pub struct EntityWriterContext {
    pub(crate) expanded_format: bool,
    pub(crate) with_prelude: WithPrelude,
    pub(crate) with_serde: WithSerde,
    pub(crate) with_copy_enums: bool,
    pub(crate) date_time_crate: DateTimeCrate,
    pub(crate) schema_name: Option<String>,
    pub(crate) lib: bool,
    pub(crate) serde_skip_hidden_column: bool,
    pub(crate) serde_skip_deserializing_primary_key: bool,
    pub(crate) model_extra_derives: TokenStream,
    pub(crate) model_extra_attributes: TokenStream,
    pub(crate) enum_extra_derives: TokenStream,
    pub(crate) enum_extra_attributes: TokenStream,
    pub(crate) seaography: bool,
    pub(crate) impl_active_model_behavior: bool,
}

impl WithSerde {
    pub fn extra_derive(&self) -> TokenStream {
        let mut extra_derive = match self {
            Self::None => {
                quote! {}
            }
            Self::Serialize => {
                quote! {
                    Serialize
                }
            }
            Self::Deserialize => {
                quote! {
                    Deserialize
                }
            }
            Self::Both => {
                quote! {
                    Serialize, Deserialize
                }
            }
        };
        if !extra_derive.is_empty() {
            extra_derive = quote! { , #extra_derive }
        }
        extra_derive
    }
}

/// Converts *_extra_derives argument to token stream
pub(crate) fn bonus_derive<T, I>(extra_derives: I) -> TokenStream
where
    T: Into<String>,
    I: IntoIterator<Item = T>,
{
    extra_derives.into_iter().map(Into::<String>::into).fold(
        TokenStream::default(),
        |acc, derive| {
            let tokens: TokenStream = derive.parse().unwrap();
            quote! { #acc, #tokens }
        },
    )
}

/// convert *_extra_attributes argument to token stream
pub(crate) fn bonus_attributes<T, I>(attributes: I) -> TokenStream
where
    T: Into<String>,
    I: IntoIterator<Item = T>,
{
    attributes.into_iter().map(Into::<String>::into).fold(
        TokenStream::default(),
        |acc, attribute| {
            let tokens: TokenStream = attribute.parse().unwrap();
            quote! {
                #acc
                #[#tokens]
            }
        },
    )
}

impl FromStr for WithPrelude {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "none" => Self::None,
            "all-allow-unused-imports" => Self::AllAllowUnusedImports,
            "all" => Self::All,
            v => {
                return Err(crate::Error::TransformError(format!(
                    "Unsupported enum variant '{v}'"
                )))
            }
        })
    }
}

impl FromStr for WithSerde {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "none" => Self::None,
            "serialize" => Self::Serialize,
            "deserialize" => Self::Deserialize,
            "both" => Self::Both,
            v => {
                return Err(crate::Error::TransformError(format!(
                    "Unsupported enum variant '{v}'"
                )))
            }
        })
    }
}

impl EntityWriterContext {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        expanded_format: bool,
        with_prelude: WithPrelude,
        with_serde: WithSerde,
        with_copy_enums: bool,
        date_time_crate: DateTimeCrate,
        schema_name: Option<String>,
        lib: bool,
        serde_skip_deserializing_primary_key: bool,
        serde_skip_hidden_column: bool,
        model_extra_derives: Vec<String>,
        model_extra_attributes: Vec<String>,
        enum_extra_derives: Vec<String>,
        enum_extra_attributes: Vec<String>,
        seaography: bool,
        impl_active_model_behavior: bool,
    ) -> Self {
        Self {
            expanded_format,
            with_prelude,
            with_serde,
            with_copy_enums,
            date_time_crate,
            schema_name,
            lib,
            serde_skip_deserializing_primary_key,
            serde_skip_hidden_column,
            model_extra_derives: bonus_derive(model_extra_derives),
            model_extra_attributes: bonus_attributes(model_extra_attributes),
            enum_extra_derives: bonus_derive(enum_extra_derives),
            enum_extra_attributes: bonus_attributes(enum_extra_attributes),
            seaography,
            impl_active_model_behavior,
        }
    }
}

impl EntityWriter {
    pub fn generate(self, context: &EntityWriterContext) -> WriterOutput {
        let mut files = Vec::new();
        files.extend(self.write_entities(context));
        let with_prelude = context.with_prelude != WithPrelude::None;
        files.push(self.write_index_file(context.lib, with_prelude, context.seaography));
        if with_prelude {
            files.push(self.write_prelude(context.with_prelude));
        }
        if !self.enums.is_empty() {
            files.push(self.write_sea_orm_active_enums(
                &context.with_serde,
                context.with_copy_enums,
                &context.enum_extra_derives,
                &context.enum_extra_attributes,
            ));
        }
        WriterOutput { files }
    }

    pub fn generate_proto(
        &self,
        context: &EntityWriterContext,
        meta: &mut BTreeMap<String, BTreeMap<String, usize>>,
    ) -> WriterOutput {
        let mut files = Vec::new();
        let mut lines = Vec::with_capacity(1024);
        let mut new_lines = Vec::with_capacity(1024);
        let mut update_lines = Vec::with_capacity(1024);
        lines.push(r##"syntax = "proto3";"##.to_owned());
        lines.push(r##"package proto.sea_orm_generate_table.query;"##.to_owned());
        new_lines.push(r##"syntax = "proto3";"##.to_owned());
        new_lines.push(r##"package proto.sea_orm_generate_table.new;"##.to_owned());
        update_lines.push(r##"syntax = "proto3";"##.to_owned());
        update_lines.push(r##"package proto.sea_orm_generate_table.update;"##.to_owned());
        lines.push("\n".to_string());
        new_lines.push("\n".to_string());
        update_lines.push("\n".to_string());
        let rust2proto_types_map: HashMap<&str, &str> = HashMap::from_iter(vec![
            ("f64", "double"),
            ("f32", "float"),
            ("i32", "int32"),
            ("i64", "int64"),
            ("u32", "uint32"),
            ("u64", "uint64"),
            ("i32", "sint32"),
            ("i64", "sint64"),
            ("u32", "fixed32"),
            ("u64", "fixed64"),
            ("i32", "sfixed32"),
            ("i64", "sfixed64"),
            ("bool", "bool"),
            ("String", "string"),
            ("DateTimeWithTimeZone", "string"),
        ]);

        self.entities.iter().for_each(|entity| {
            let msg_name = entity.get_table_name_camel_case();
            let mut all_msgs = vec![msg_name.clone()];
            if entity.columns.iter().find(|col| col.name == "id").is_some() {
                // 如存在ID列，则此数据库表可以额外创建新增、修改两个message
                all_msgs.extend([format!("New{msg_name}"), format!("Update{msg_name}")]);
            }
            for msg in all_msgs {
                let mut entity_msg_lines = Vec::with_capacity(entity.columns.len());
                entity_msg_lines.push(format!("message {msg}{{"));
                let is_new = msg.starts_with("New");
                let is_update = msg.starts_with("Update");
                let entity_meta = meta.entry(msg.clone()).or_default();
                entity_msg_lines.extend(entity.columns.iter().enumerate().filter_map(
                    |(idx, col)| {
                        let mut col_type = col
                            .get_rs_type_no_option(&context.date_time_crate)
                            .to_string()
                            .replace(' ', "");

                        if ["deleted_at", "left", "right", "tag", "level"]
                            .contains(&col.name.as_str())
                        {
                            // 这个字段不需要写到proto文件中
                            return None;
                        }
                        // 这几个也不需要写进去， 因为可以从jwt 里获取到每次的操作用户, 但是查询的时候还是需要
                        else if (is_new || is_update)
                            && ["creator", "editor", "id", "created_at", "updated_at"]
                                .contains(&col.name.as_str())
                        {
                            return None;
                        }
                        let proto_idx = entity_meta.entry(col.name.clone()).or_insert(idx + 1);
                        let optional = if col.not_null { "" } else { "optional " };
                        let repeated = if col_type.starts_with("Vec<") {
                            col_type = col_type.replace("Vec<", "").replace(">", "");
                            "repeated "
                        } else {
                            ""
                        };
                        match rust2proto_types_map.get(col_type.as_str()) {
                            Some(proto_type) => Some(format!(
                                "    {optional}{repeated}{proto_type} {} = {};",
                                col.name, *proto_idx
                            )),
                            None => {
                                eprintln!("不支持的类型{}", col.name.as_str());
                                None
                            }
                        }
                    },
                ));
                entity_msg_lines.push(format!("}}\n"));
                if is_new {
                    new_lines.extend(entity_msg_lines);
                } else if is_update {
                    update_lines.extend(entity_msg_lines);
                } else {
                    lines.extend(entity_msg_lines);
                }
            }
        });
        // .for_each(|entity_lines| {
        //     lines.extend(entity_lines);
        // });
        files.push(OutputFile {
            name: "generate_sea_orm_query.proto".to_string(),
            content: lines.join("\n"),
        });
        files.push(OutputFile {
            name: "generate_sea_orm_new.proto".to_string(),
            content: new_lines.join("\n"),
        });
        files.push(OutputFile {
            name: "generate_sea_orm_update.proto".to_string(),
            content: update_lines.join("\n"),
        });
        WriterOutput { files }
    }
    fn lr_tree_tables(
        &self,
        // meta: &mut BTreeMap<String, BTreeMap<String, usize>>,
    ) -> Vec<String> {
        let tree_fields = ["right", "left", "level", "tag"];
        self.entities
            .iter()
            .filter(|entity| {
                entity
                    .columns
                    .iter()
                    .filter(|col| tree_fields.contains(&col.name.as_str()))
                    .count()
                    == tree_fields.len()
            })
            .map(|entity| entity.get_table_name_camel_case().to_snake_case())
            .collect::<Vec<_>>()
    }
    pub fn generate_tree_view_tables(
        &self,
        context: &EntityWriterContext,
        // meta: &mut BTreeMap<String, BTreeMap<String, usize>>,
    ) -> WriterOutput {
        let mut files = Vec::new();
        files.push(OutputFile {
            name: "left_right_tree_tables.txt".to_string(),
            content: self.lr_tree_tables().join(","),
        });
        WriterOutput { files }
    }
    pub fn generate_derive_auto_simple_curd_api(
        &self,
        context: &EntityWriterContext,
        table_validate_map: &HashMap<String, String>,
        // meta: &mut BTreeMap<String, BTreeMap<String, usize>>,
    ) -> WriterOutput {
        let mut files = Vec::new();
        let mut lines = Vec::with_capacity(1024);
        lines.push(r##"use cus_marco::DeriveCusAutoSimpleCrudIeApi;"##.to_owned());
        lines.push(r##"use super::*;"##.to_owned());
        lines.push("\n".to_owned());
        let lrt_tables = self.lr_tree_tables();
        // 只自动创建那种有id列的表
        self.entities.iter().filter(|entity| entity.columns.iter().find(|col| col.name == "id").is_some())
            .for_each(|entity| {
                let table = entity.get_table_name_camel_case();
                let is_lrt = lrt_tables.contains(&table.to_snake_case());
                let insert_query = if is_lrt {
                    "?parent_id=1122"
                }else{
                  ""
                };
                lines.extend([
                    format!("///简单的数据库表{}的http crud操作,格式如下:", table.to_snake_case()),
                    format!("///\tGET 查询全部  {{API_CONTEXT_PATH}}/{}/all ， 需要用户具有query权限", table.to_snake_case()),
                    format!("///\tPOST 新增数据  {{API_CONTEXT_PATH}}/{}{} ， 需要用户具有new权限 ", table.to_snake_case(), insert_query),
                    format!("///\tPUT  修改数据  {{API_CONTEXT_PATH}}/{} ，  需要用户具有update权限", table.to_snake_case()),
                    format!("///\tDELETE 删除某条数据  {{API_CONTEXT_PATH}}/{}/:id， 需要用户具有delete权限 ", table.to_snake_case()),
                ]);
                if is_lrt {
                    lines.extend([
                        "///基于左右值编码的表还有以下额外接口:".to_string(),
                        format!("///\tGET 查询根节点  {{API_CONTEXT_PATH}}/{}/root ， 需要用户具有query权限", table.to_snake_case()),
                        format!("///\tGET 查询儿子节点或者所有子节点  {{API_CONTEXT_PATH}}/{}/root?parent_id=xx&&only_sons=true|false ， 需要用户具有query权限", table.to_snake_case()),
                    ]);
                }
                lines.push("#[derive(DeriveCusAutoSimpleCrudIeApi)]".to_string());
                if let Some(p)  = table_validate_map.get( table.to_snake_case().as_str() ) {
                    let p = p.trim();
                    if p.is_empty() {
                        // 空字符串的话，使用默认的验证器
                        // 添加专门验证的宏
                        lines.push("#[yxt(validator)]".to_string());
                    }else{
                        lines.push(format!("#[yxt(validator=\"{p}\")]"));
                    }
                }

                lines.push(format!("struct {table}AutoSimpleCrudIeApi;\n"));
            });
        // .for_each(|entity_lines| {
        //     lines.extend(entity_lines);
        // });
        files.push(OutputFile {
            name: "generate_crud_ie.rs".to_string(),
            content: lines.join("\n"),
        });

        WriterOutput { files }
    }

    pub fn write_entities(&self, context: &EntityWriterContext) -> Vec<OutputFile> {
        self.entities
            .iter()
            .map(|entity| {
                let entity_file = format!("{}.rs", entity.get_table_name_snake_case());
                let column_info = entity
                    .columns
                    .iter()
                    .map(|column| column.get_info(&context.date_time_crate))
                    .collect::<Vec<String>>();
                // Serde must be enabled to use this
                let serde_skip_deserializing_primary_key = context
                    .serde_skip_deserializing_primary_key
                    && matches!(context.with_serde, WithSerde::Both | WithSerde::Deserialize);
                let serde_skip_hidden_column = context.serde_skip_hidden_column
                    && matches!(
                        context.with_serde,
                        WithSerde::Both | WithSerde::Serialize | WithSerde::Deserialize
                    );

                info!("Generating {}", entity_file);
                for info in column_info.iter() {
                    info!("    > {}", info);
                }

                let mut lines = Vec::new();
                Self::write_doc_comment(&mut lines);
                let code_blocks = if context.expanded_format {
                    Self::gen_expanded_code_blocks(
                        entity,
                        &context.with_serde,
                        &context.date_time_crate,
                        &context.schema_name,
                        serde_skip_deserializing_primary_key,
                        serde_skip_hidden_column,
                        &context.model_extra_derives,
                        &context.model_extra_attributes,
                        context.seaography,
                        context.impl_active_model_behavior,
                    )
                } else {
                    Self::gen_compact_code_blocks(
                        entity,
                        &context.with_serde,
                        &context.date_time_crate,
                        &context.schema_name,
                        serde_skip_deserializing_primary_key,
                        serde_skip_hidden_column,
                        &context.model_extra_derives,
                        &context.model_extra_attributes,
                        context.seaography,
                        context.impl_active_model_behavior,
                    )
                };
                Self::write(&mut lines, code_blocks);
                OutputFile {
                    name: entity_file,
                    content: lines.join("\n\n"),
                }
            })
            .collect()
    }

    pub fn write_index_file(&self, lib: bool, prelude: bool, seaography: bool) -> OutputFile {
        let mut lines = Vec::new();
        Self::write_doc_comment(&mut lines);
        let code_blocks: Vec<TokenStream> = self.entities.iter().map(Self::gen_mod).collect();
        if prelude {
            Self::write(
                &mut lines,
                vec![quote! {
                    pub mod prelude;
                }],
            );
            lines.push("".to_owned());
        }
        Self::write(&mut lines, code_blocks);
        if !self.enums.is_empty() {
            Self::write(
                &mut lines,
                vec![quote! {
                    pub mod sea_orm_active_enums;
                }],
            );
        }

        if seaography {
            lines.push("".to_owned());
            let ts = Self::gen_seaography_entity_mod(&self.entities, &self.enums);
            Self::write(&mut lines, vec![ts]);
        }

        let file_name = match lib {
            true => "lib.rs".to_owned(),
            false => "mod.rs".to_owned(),
        };

        OutputFile {
            name: file_name,
            content: lines.join("\n"),
        }
    }

    pub fn write_prelude(&self, with_prelude: WithPrelude) -> OutputFile {
        let mut lines = Vec::new();
        Self::write_doc_comment(&mut lines);
        if with_prelude == WithPrelude::AllAllowUnusedImports {
            Self::write_allow_unused_imports(&mut lines)
        }
        let code_blocks = self.entities.iter().map(Self::gen_prelude_use).collect();
        Self::write(&mut lines, code_blocks);
        OutputFile {
            name: "prelude.rs".to_owned(),
            content: lines.join("\n"),
        }
    }

    pub fn write_sea_orm_active_enums(
        &self,
        with_serde: &WithSerde,
        with_copy_enums: bool,
        extra_derives: &TokenStream,
        extra_attributes: &TokenStream,
    ) -> OutputFile {
        let mut lines = Vec::new();
        Self::write_doc_comment(&mut lines);
        Self::write(&mut lines, vec![Self::gen_import(with_serde)]);
        lines.push("".to_owned());
        let code_blocks = self
            .enums
            .values()
            .map(|active_enum| {
                active_enum.impl_active_enum(
                    with_serde,
                    with_copy_enums,
                    extra_derives,
                    extra_attributes,
                )
            })
            .collect();
        Self::write(&mut lines, code_blocks);
        OutputFile {
            name: "sea_orm_active_enums.rs".to_owned(),
            content: lines.join("\n"),
        }
    }

    pub fn write(lines: &mut Vec<String>, code_blocks: Vec<TokenStream>) {
        lines.extend(
            code_blocks
                .into_iter()
                .map(|code_block| code_block.to_string())
                .collect::<Vec<_>>(),
        );
    }

    pub fn write_doc_comment(lines: &mut Vec<String>) {
        let ver = env!("CARGO_PKG_VERSION");
        let comments = vec![format!(
            "//! `SeaORM` Entity, @generated by sea-orm-codegen {ver}"
        )];
        lines.extend(comments);
        lines.push("".to_owned());
    }

    pub fn write_allow_unused_imports(lines: &mut Vec<String>) {
        lines.extend(vec!["#![allow(unused_imports)]".to_string()]);
        lines.push("".to_owned());
    }

    #[allow(clippy::too_many_arguments)]
    pub fn gen_expanded_code_blocks(
        entity: &Entity,
        with_serde: &WithSerde,
        date_time_crate: &DateTimeCrate,
        schema_name: &Option<String>,
        serde_skip_deserializing_primary_key: bool,
        serde_skip_hidden_column: bool,
        model_extra_derives: &TokenStream,
        model_extra_attributes: &TokenStream,
        seaography: bool,
        impl_active_model_behavior: bool,
    ) -> Vec<TokenStream> {
        let mut imports = Self::gen_import(with_serde);
        imports.extend(Self::gen_import_active_enum(entity));
        let mut code_blocks = vec![
            imports,
            Self::gen_entity_struct(),
            Self::gen_impl_entity_name(entity, schema_name),
            Self::gen_model_struct(
                entity,
                with_serde,
                date_time_crate,
                serde_skip_deserializing_primary_key,
                serde_skip_hidden_column,
                model_extra_derives,
                model_extra_attributes,
            ),
            Self::gen_column_enum(entity),
            Self::gen_primary_key_enum(entity),
            Self::gen_impl_primary_key(entity, date_time_crate),
            Self::gen_relation_enum(entity),
            Self::gen_impl_column_trait(entity),
            Self::gen_impl_relation_trait(entity),
        ];
        code_blocks.extend(Self::gen_impl_related(entity));
        code_blocks.extend(Self::gen_impl_conjunct_related(entity));
        if impl_active_model_behavior {
            code_blocks.extend([Self::impl_active_model_behavior()]);
        }
        if seaography {
            code_blocks.extend([Self::gen_related_entity(entity)]);
        }
        code_blocks
    }

    #[allow(clippy::too_many_arguments)]
    pub fn gen_compact_code_blocks(
        entity: &Entity,
        with_serde: &WithSerde,
        date_time_crate: &DateTimeCrate,
        schema_name: &Option<String>,
        serde_skip_deserializing_primary_key: bool,
        serde_skip_hidden_column: bool,
        model_extra_derives: &TokenStream,
        model_extra_attributes: &TokenStream,
        seaography: bool,
        impl_active_model_behavior: bool,
    ) -> Vec<TokenStream> {
        let mut imports = Self::gen_import(with_serde);
        imports.extend(Self::gen_import_active_enum(entity));
        let mut code_blocks = vec![
            imports,
            Self::gen_compact_model_struct(
                entity,
                with_serde,
                date_time_crate,
                schema_name,
                serde_skip_deserializing_primary_key,
                serde_skip_hidden_column,
                model_extra_derives,
                model_extra_attributes,
            ),
            Self::gen_compact_relation_enum(entity),
        ];
        code_blocks.extend(Self::gen_impl_related(entity));
        code_blocks.extend(Self::gen_impl_conjunct_related(entity));
        if impl_active_model_behavior {
            code_blocks.extend([Self::impl_active_model_behavior()]);
        }
        if seaography {
            code_blocks.extend([Self::gen_related_entity(entity)]);
        }
        code_blocks
    }

    pub fn gen_import(with_serde: &WithSerde) -> TokenStream {
        let prelude_import = quote!(
            use sea_orm::entity::prelude::*;
        );

        match with_serde {
            WithSerde::None => prelude_import,
            WithSerde::Serialize => {
                quote! {
                    #prelude_import
                    use serde::Serialize;
                }
            }
            WithSerde::Deserialize => {
                quote! {
                    #prelude_import
                    use serde::Deserialize;
                }
            }
            WithSerde::Both => {
                quote! {
                    #prelude_import
                    use serde::{Deserialize,Serialize};
                }
            }
        }
    }

    pub fn gen_entity_struct() -> TokenStream {
        quote! {
            #[derive(Copy, Clone, Default, Debug, DeriveEntity)]
            pub struct Entity;
        }
    }

    pub fn gen_impl_entity_name(entity: &Entity, schema_name: &Option<String>) -> TokenStream {
        let schema_name = match Self::gen_schema_name(schema_name) {
            Some(schema_name) => quote! {
                fn schema_name(&self) -> Option<&str> {
                    Some(#schema_name)
                }
            },
            None => quote! {},
        };
        let table_name = entity.table_name.as_str();
        let table_name = quote! {
            fn table_name(&self) -> &str {
                #table_name
            }
        };
        quote! {
            impl EntityName for Entity {
                #schema_name
                #table_name
            }
        }
    }

    pub fn gen_import_active_enum(entity: &Entity) -> TokenStream {
        entity
            .columns
            .iter()
            .fold(
                (TokenStream::new(), Vec::new()),
                |(mut ts, mut enums), col| {
                    if let sea_query::ColumnType::Enum { name, .. } = col.get_inner_col_type() {
                        if !enums.contains(&name) {
                            enums.push(name);
                            let enum_name =
                                format_ident!("{}", name.to_string().to_upper_camel_case());
                            ts.extend([quote! {
                                use super::sea_orm_active_enums::#enum_name;
                            }]);
                        }
                    }
                    (ts, enums)
                },
            )
            .0
    }

    pub fn gen_model_struct(
        entity: &Entity,
        with_serde: &WithSerde,
        date_time_crate: &DateTimeCrate,
        serde_skip_deserializing_primary_key: bool,
        serde_skip_hidden_column: bool,
        model_extra_derives: &TokenStream,
        model_extra_attributes: &TokenStream,
    ) -> TokenStream {
        let column_names_snake_case = entity.get_column_names_snake_case();
        let column_rs_types = entity.get_column_rs_types(date_time_crate);
        let if_eq_needed = entity.get_eq_needed();
        let serde_attributes = entity.get_column_serde_attributes(
            serde_skip_deserializing_primary_key,
            serde_skip_hidden_column,
        );
        let extra_derive = with_serde.extra_derive();

        quote! {
            #[derive(Clone, Debug, PartialEq, DeriveModel, DeriveActiveModel #if_eq_needed #extra_derive #model_extra_derives)]
            #model_extra_attributes
            pub struct Model {
                #(
                    #serde_attributes
                    pub #column_names_snake_case: #column_rs_types,
                )*
            }
        }
    }

    pub fn gen_column_enum(entity: &Entity) -> TokenStream {
        let column_variants = entity.columns.iter().map(|col| {
            let variant = col.get_name_camel_case();
            let mut variant = quote! { #variant };
            if !col.is_snake_case_name() {
                let column_name = &col.name;
                variant = quote! {
                    #[sea_orm(column_name = #column_name)]
                    #variant
                };
            }
            variant
        });
        quote! {
            #[derive(Copy, Clone, Debug, EnumIter, DeriveColumn)]
            pub enum Column {
                #(#column_variants,)*
            }
        }
    }

    pub fn gen_primary_key_enum(entity: &Entity) -> TokenStream {
        let primary_key_names_camel_case = entity.get_primary_key_names_camel_case();
        quote! {
            #[derive(Copy, Clone, Debug, EnumIter, DerivePrimaryKey)]
            pub enum PrimaryKey {
                #(#primary_key_names_camel_case,)*
            }
        }
    }

    pub fn gen_impl_primary_key(entity: &Entity, date_time_crate: &DateTimeCrate) -> TokenStream {
        let primary_key_auto_increment = entity.get_primary_key_auto_increment();
        let value_type = entity.get_primary_key_rs_type(date_time_crate);
        quote! {
            impl PrimaryKeyTrait for PrimaryKey {
                type ValueType = #value_type;

                fn auto_increment() -> bool {
                    #primary_key_auto_increment
                }
            }
        }
    }

    pub fn gen_relation_enum(entity: &Entity) -> TokenStream {
        let relation_enum_name = entity.get_relation_enum_name();
        quote! {
            #[derive(Copy, Clone, Debug, EnumIter)]
            pub enum Relation {
                #(#relation_enum_name,)*
            }
        }
    }

    pub fn gen_impl_column_trait(entity: &Entity) -> TokenStream {
        let column_names_camel_case = entity.get_column_names_camel_case();
        let column_defs = entity.get_column_defs();
        quote! {
            impl ColumnTrait for Column {
                type EntityName = Entity;

                fn def(&self) -> ColumnDef {
                    match self {
                        #(Self::#column_names_camel_case => #column_defs,)*
                    }
                }
            }
        }
    }

    pub fn gen_impl_relation_trait(entity: &Entity) -> TokenStream {
        let relation_enum_name = entity.get_relation_enum_name();
        let relation_defs = entity.get_relation_defs();
        let quoted = if relation_enum_name.is_empty() {
            quote! {
                panic!("No RelationDef")
            }
        } else {
            quote! {
                match self {
                    #(Self::#relation_enum_name => #relation_defs,)*
                }
            }
        };
        quote! {
            impl RelationTrait for Relation {
                fn def(&self) -> RelationDef {
                    #quoted
                }
            }
        }
    }

    pub fn gen_impl_related(entity: &Entity) -> Vec<TokenStream> {
        entity
            .relations
            .iter()
            .filter(|rel| !rel.self_referencing && rel.num_suffix == 0 && rel.impl_related)
            .map(|rel| {
                let enum_name = rel.get_enum_name();
                let module_name = rel.get_module_name();
                let inner = quote! {
                    fn to() -> RelationDef {
                        Relation::#enum_name.def()
                    }
                };
                if module_name.is_some() {
                    quote! {
                        impl Related<super::#module_name::Entity> for Entity { #inner }
                    }
                } else {
                    quote! {
                        impl Related<Entity> for Entity { #inner }
                    }
                }
            })
            .collect()
    }

    /// Used to generate `enum RelatedEntity` that is useful to the Seaography project
    pub fn gen_related_entity(entity: &Entity) -> TokenStream {
        let related_enum_name = entity.get_related_entity_enum_name();
        let related_attrs = entity.get_related_entity_attrs();

        quote! {
            #[derive(Copy, Clone, Debug, EnumIter, DeriveRelatedEntity)]
            pub enum RelatedEntity {
                #(
                    #related_attrs
                    #related_enum_name
                ),*
            }
        }
    }

    pub fn gen_impl_conjunct_related(entity: &Entity) -> Vec<TokenStream> {
        let table_name_camel_case = entity.get_table_name_camel_case_ident();
        let via_snake_case = entity.get_conjunct_relations_via_snake_case();
        let to_snake_case = entity.get_conjunct_relations_to_snake_case();
        let to_upper_camel_case = entity.get_conjunct_relations_to_upper_camel_case();
        via_snake_case
            .into_iter()
            .zip(to_snake_case)
            .zip(to_upper_camel_case)
            .map(|((via_snake_case, to_snake_case), to_upper_camel_case)| {
                quote! {
                    impl Related<super::#to_snake_case::Entity> for Entity {
                        fn to() -> RelationDef {
                            super::#via_snake_case::Relation::#to_upper_camel_case.def()
                        }

                        fn via() -> Option<RelationDef> {
                            Some(super::#via_snake_case::Relation::#table_name_camel_case.def().rev())
                        }
                    }
                }
            })
            .collect()
    }

    pub fn impl_active_model_behavior() -> TokenStream {
        quote! {
            impl ActiveModelBehavior for ActiveModel {}
        }
    }

    pub fn gen_mod(entity: &Entity) -> TokenStream {
        let table_name_snake_case_ident = format_ident!(
            "{}",
            escape_rust_keyword(entity.get_table_name_snake_case_ident())
        );
        quote! {
            pub mod #table_name_snake_case_ident;
        }
    }

    pub fn gen_seaography_entity_mod(
        entities: &[Entity],
        enums: &BTreeMap<String, ActiveEnum>,
    ) -> TokenStream {
        let mut ts = TokenStream::new();
        for entity in entities {
            let table_name_snake_case_ident = format_ident!(
                "{}",
                escape_rust_keyword(entity.get_table_name_snake_case_ident())
            );
            ts = quote! {
                #ts
                #table_name_snake_case_ident,
            }
        }
        ts = quote! {
            seaography::register_entity_modules!([
                #ts
            ]);
        };

        let mut enum_ts = TokenStream::new();
        for active_enum in enums.values() {
            let enum_name = &active_enum.enum_name.to_string();
            let enum_iden = format_ident!("{}", enum_name.to_upper_camel_case());
            enum_ts = quote! {
                #enum_ts
                sea_orm_active_enums::#enum_iden,
            }
        }
        if !enum_ts.is_empty() {
            ts = quote! {
                #ts

                seaography::register_active_enums!([
                    #enum_ts
                ]);
            };
        }
        ts
    }

    pub fn gen_prelude_use(entity: &Entity) -> TokenStream {
        let table_name_snake_case_ident = entity.get_table_name_snake_case_ident();
        let table_name_camel_case_ident = entity.get_table_name_camel_case_ident();
        quote! {
            pub use super::#table_name_snake_case_ident::Entity as #table_name_camel_case_ident;
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn gen_compact_model_struct(
        entity: &Entity,
        with_serde: &WithSerde,
        date_time_crate: &DateTimeCrate,
        schema_name: &Option<String>,
        serde_skip_deserializing_primary_key: bool,
        serde_skip_hidden_column: bool,
        model_extra_derives: &TokenStream,
        model_extra_attributes: &TokenStream,
    ) -> TokenStream {
        let table_name = entity.table_name.as_str();
        let column_names_snake_case = entity.get_column_names_snake_case();
        let column_rs_types = entity.get_column_rs_types(date_time_crate);
        let if_eq_needed = entity.get_eq_needed();
        let primary_keys: Vec<String> = entity
            .primary_keys
            .iter()
            .map(|pk| pk.name.clone())
            .collect();
        let attrs: Vec<TokenStream> = entity
            .columns
            .iter()
            .map(|col| {
                let mut attrs: Punctuated<_, Comma> = Punctuated::new();
                let is_primary_key = primary_keys.contains(&col.name);
                if !col.is_snake_case_name() {
                    let column_name = &col.name;
                    attrs.push(quote! { column_name = #column_name });
                }
                if is_primary_key {
                    attrs.push(quote! { primary_key });
                    if !col.auto_increment {
                        attrs.push(quote! { auto_increment = false });
                    }
                }
                if let Some(ts) = col.get_col_type_attrs() {
                    attrs.extend([ts]);
                    if !col.not_null {
                        attrs.push(quote! { nullable });
                    }
                };
                if col.unique {
                    attrs.push(quote! { unique });
                }
                let mut ts = quote! {};
                if !attrs.is_empty() {
                    for (i, attr) in attrs.into_iter().enumerate() {
                        if i > 0 {
                            ts = quote! { #ts, };
                        }
                        ts = quote! { #ts #attr };
                    }
                    ts = quote! { #[sea_orm(#ts)] };
                }
                let serde_attribute = col.get_serde_attribute(
                    is_primary_key,
                    serde_skip_deserializing_primary_key,
                    serde_skip_hidden_column,
                );
                ts = quote! {
                    #ts
                    #serde_attribute
                };
                ts
            })
            .collect();
        let schema_name = match Self::gen_schema_name(schema_name) {
            Some(schema_name) => quote! {
                schema_name = #schema_name,
            },
            None => quote! {},
        };
        let extra_derive = with_serde.extra_derive();

        quote! {
            #[derive(Clone, Debug, PartialEq, DeriveEntityModel #if_eq_needed #extra_derive #model_extra_derives)]
            #[sea_orm(
                #schema_name
                table_name = #table_name
            )]
            #model_extra_attributes
            pub struct Model {
                #(
                    #attrs
                    pub #column_names_snake_case: #column_rs_types,
                )*
            }
        }
    }

    pub fn gen_compact_relation_enum(entity: &Entity) -> TokenStream {
        let relation_enum_name = entity.get_relation_enum_name();
        let attrs = entity.get_relation_attrs();
        quote! {
            #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
            pub enum Relation {
                #(
                    #attrs
                    #relation_enum_name,
                )*
            }
        }
    }

    pub fn gen_schema_name(schema_name: &Option<String>) -> Option<TokenStream> {
        schema_name
            .as_ref()
            .map(|schema_name| quote! { #schema_name })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        entity::writer::{bonus_attributes, bonus_derive},
        Column, ConjunctRelation, DateTimeCrate, Entity, EntityWriter, PrimaryKey, Relation,
        RelationType, WithSerde,
    };
    use pretty_assertions::assert_eq;
    use proc_macro2::TokenStream;
    use quote::quote;
    use sea_query::{Alias, ColumnType, ForeignKeyAction, RcOrArc, SeaRc, StringLen};
    use std::io::{self, BufRead, BufReader, Read};

    fn setup() -> Vec<Entity> {
        vec![
            Entity {
                table_name: "cake".to_owned(),
                columns: vec![
                    Column {
                        name: "id".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: true,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "name".to_owned(),
                        col_type: ColumnType::Text,
                        auto_increment: false,
                        not_null: false,
                        unique: false,
                    },
                ],
                relations: vec![Relation {
                    ref_table: "fruit".to_owned(),
                    columns: vec![],
                    ref_columns: vec![],
                    rel_type: RelationType::HasMany,
                    on_delete: None,
                    on_update: None,
                    self_referencing: false,
                    num_suffix: 0,
                    impl_related: true,
                }],
                conjunct_relations: vec![ConjunctRelation {
                    via: "cake_filling".to_owned(),
                    to: "filling".to_owned(),
                }],
                primary_keys: vec![PrimaryKey {
                    name: "id".to_owned(),
                }],
            },
            Entity {
                table_name: "_cake_filling_".to_owned(),
                columns: vec![
                    Column {
                        name: "cake_id".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "filling_id".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                ],
                relations: vec![
                    Relation {
                        ref_table: "cake".to_owned(),
                        columns: vec!["cake_id".to_owned()],
                        ref_columns: vec!["id".to_owned()],
                        rel_type: RelationType::BelongsTo,
                        on_delete: Some(ForeignKeyAction::Cascade),
                        on_update: Some(ForeignKeyAction::Cascade),
                        self_referencing: false,
                        num_suffix: 0,
                        impl_related: true,
                    },
                    Relation {
                        ref_table: "filling".to_owned(),
                        columns: vec!["filling_id".to_owned()],
                        ref_columns: vec!["id".to_owned()],
                        rel_type: RelationType::BelongsTo,
                        on_delete: Some(ForeignKeyAction::Cascade),
                        on_update: Some(ForeignKeyAction::Cascade),
                        self_referencing: false,
                        num_suffix: 0,
                        impl_related: true,
                    },
                ],
                conjunct_relations: vec![],
                primary_keys: vec![
                    PrimaryKey {
                        name: "cake_id".to_owned(),
                    },
                    PrimaryKey {
                        name: "filling_id".to_owned(),
                    },
                ],
            },
            Entity {
                table_name: "cake_filling_price".to_owned(),
                columns: vec![
                    Column {
                        name: "cake_id".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "filling_id".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "price".to_owned(),
                        col_type: ColumnType::Decimal(None),
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                ],
                relations: vec![Relation {
                    ref_table: "cake_filling".to_owned(),
                    columns: vec!["cake_id".to_owned(), "filling_id".to_owned()],
                    ref_columns: vec!["cake_id".to_owned(), "filling_id".to_owned()],
                    rel_type: RelationType::BelongsTo,
                    on_delete: None,
                    on_update: None,
                    self_referencing: false,
                    num_suffix: 0,
                    impl_related: true,
                }],
                conjunct_relations: vec![],
                primary_keys: vec![
                    PrimaryKey {
                        name: "cake_id".to_owned(),
                    },
                    PrimaryKey {
                        name: "filling_id".to_owned(),
                    },
                ],
            },
            Entity {
                table_name: "filling".to_owned(),
                columns: vec![
                    Column {
                        name: "id".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: true,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "name".to_owned(),
                        col_type: ColumnType::String(StringLen::N(255)),
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                ],
                relations: vec![],
                conjunct_relations: vec![ConjunctRelation {
                    via: "cake_filling".to_owned(),
                    to: "cake".to_owned(),
                }],
                primary_keys: vec![PrimaryKey {
                    name: "id".to_owned(),
                }],
            },
            Entity {
                table_name: "fruit".to_owned(),
                columns: vec![
                    Column {
                        name: "id".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: true,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "name".to_owned(),
                        col_type: ColumnType::String(StringLen::N(255)),
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "cake_id".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: false,
                        not_null: false,
                        unique: false,
                    },
                ],
                relations: vec![
                    Relation {
                        ref_table: "cake".to_owned(),
                        columns: vec!["cake_id".to_owned()],
                        ref_columns: vec!["id".to_owned()],
                        rel_type: RelationType::BelongsTo,
                        on_delete: None,
                        on_update: None,
                        self_referencing: false,
                        num_suffix: 0,
                        impl_related: true,
                    },
                    Relation {
                        ref_table: "vendor".to_owned(),
                        columns: vec![],
                        ref_columns: vec![],
                        rel_type: RelationType::HasMany,
                        on_delete: None,
                        on_update: None,
                        self_referencing: false,
                        num_suffix: 0,
                        impl_related: true,
                    },
                ],
                conjunct_relations: vec![],
                primary_keys: vec![PrimaryKey {
                    name: "id".to_owned(),
                }],
            },
            Entity {
                table_name: "vendor".to_owned(),
                columns: vec![
                    Column {
                        name: "id".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: true,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "_name_".to_owned(),
                        col_type: ColumnType::String(StringLen::N(255)),
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "fruitId".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: false,
                        not_null: false,
                        unique: false,
                    },
                ],
                relations: vec![Relation {
                    ref_table: "fruit".to_owned(),
                    columns: vec!["fruitId".to_owned()],
                    ref_columns: vec!["id".to_owned()],
                    rel_type: RelationType::BelongsTo,
                    on_delete: None,
                    on_update: None,
                    self_referencing: false,
                    num_suffix: 0,
                    impl_related: true,
                }],
                conjunct_relations: vec![],
                primary_keys: vec![PrimaryKey {
                    name: "id".to_owned(),
                }],
            },
            Entity {
                table_name: "rust_keyword".to_owned(),
                columns: vec![
                    Column {
                        name: "id".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: true,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "testing".to_owned(),
                        col_type: ColumnType::TinyInteger,
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "rust".to_owned(),
                        col_type: ColumnType::TinyUnsigned,
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "keywords".to_owned(),
                        col_type: ColumnType::SmallInteger,
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "type".to_owned(),
                        col_type: ColumnType::SmallUnsigned,
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "typeof".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "crate".to_owned(),
                        col_type: ColumnType::Unsigned,
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "self".to_owned(),
                        col_type: ColumnType::BigInteger,
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "self_id1".to_owned(),
                        col_type: ColumnType::BigUnsigned,
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "self_id2".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "fruit_id1".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "fruit_id2".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "cake_id".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                ],
                relations: vec![
                    Relation {
                        ref_table: "rust_keyword".to_owned(),
                        columns: vec!["self_id1".to_owned()],
                        ref_columns: vec!["id".to_owned()],
                        rel_type: RelationType::BelongsTo,
                        on_delete: None,
                        on_update: None,
                        self_referencing: true,
                        num_suffix: 1,
                        impl_related: true,
                    },
                    Relation {
                        ref_table: "rust_keyword".to_owned(),
                        columns: vec!["self_id2".to_owned()],
                        ref_columns: vec!["id".to_owned()],
                        rel_type: RelationType::BelongsTo,
                        on_delete: None,
                        on_update: None,
                        self_referencing: true,
                        num_suffix: 2,
                        impl_related: true,
                    },
                    Relation {
                        ref_table: "fruit".to_owned(),
                        columns: vec!["fruit_id1".to_owned()],
                        ref_columns: vec!["id".to_owned()],
                        rel_type: RelationType::BelongsTo,
                        on_delete: None,
                        on_update: None,
                        self_referencing: false,
                        num_suffix: 1,
                        impl_related: true,
                    },
                    Relation {
                        ref_table: "fruit".to_owned(),
                        columns: vec!["fruit_id2".to_owned()],
                        ref_columns: vec!["id".to_owned()],
                        rel_type: RelationType::BelongsTo,
                        on_delete: None,
                        on_update: None,
                        self_referencing: false,
                        num_suffix: 2,
                        impl_related: true,
                    },
                    Relation {
                        ref_table: "cake".to_owned(),
                        columns: vec!["cake_id".to_owned()],
                        ref_columns: vec!["id".to_owned()],
                        rel_type: RelationType::BelongsTo,
                        on_delete: None,
                        on_update: None,
                        self_referencing: false,
                        num_suffix: 0,
                        impl_related: true,
                    },
                ],
                conjunct_relations: vec![],
                primary_keys: vec![PrimaryKey {
                    name: "id".to_owned(),
                }],
            },
            Entity {
                table_name: "cake_with_float".to_owned(),
                columns: vec![
                    Column {
                        name: "id".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: true,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "name".to_owned(),
                        col_type: ColumnType::Text,
                        auto_increment: false,
                        not_null: false,
                        unique: false,
                    },
                    Column {
                        name: "price".to_owned(),
                        col_type: ColumnType::Float,
                        auto_increment: false,
                        not_null: false,
                        unique: false,
                    },
                ],
                relations: vec![Relation {
                    ref_table: "fruit".to_owned(),
                    columns: vec![],
                    ref_columns: vec![],
                    rel_type: RelationType::HasMany,
                    on_delete: None,
                    on_update: None,
                    self_referencing: false,
                    num_suffix: 0,
                    impl_related: true,
                }],
                conjunct_relations: vec![ConjunctRelation {
                    via: "cake_filling".to_owned(),
                    to: "filling".to_owned(),
                }],
                primary_keys: vec![PrimaryKey {
                    name: "id".to_owned(),
                }],
            },
            Entity {
                table_name: "cake_with_double".to_owned(),
                columns: vec![
                    Column {
                        name: "id".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: true,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "name".to_owned(),
                        col_type: ColumnType::Text,
                        auto_increment: false,
                        not_null: false,
                        unique: false,
                    },
                    Column {
                        name: "price".to_owned(),
                        col_type: ColumnType::Double,
                        auto_increment: false,
                        not_null: false,
                        unique: false,
                    },
                ],
                relations: vec![Relation {
                    ref_table: "fruit".to_owned(),
                    columns: vec![],
                    ref_columns: vec![],
                    rel_type: RelationType::HasMany,
                    on_delete: None,
                    on_update: None,
                    self_referencing: false,
                    num_suffix: 0,
                    impl_related: true,
                }],
                conjunct_relations: vec![ConjunctRelation {
                    via: "cake_filling".to_owned(),
                    to: "filling".to_owned(),
                }],
                primary_keys: vec![PrimaryKey {
                    name: "id".to_owned(),
                }],
            },
            Entity {
                table_name: "collection".to_owned(),
                columns: vec![
                    Column {
                        name: "id".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: true,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "integers".to_owned(),
                        col_type: ColumnType::Array(RcOrArc::new(ColumnType::Integer)),
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "integers_opt".to_owned(),
                        col_type: ColumnType::Array(RcOrArc::new(ColumnType::Integer)),
                        auto_increment: false,
                        not_null: false,
                        unique: false,
                    },
                ],
                relations: vec![],
                conjunct_relations: vec![],
                primary_keys: vec![PrimaryKey {
                    name: "id".to_owned(),
                }],
            },
            Entity {
                table_name: "collection_float".to_owned(),
                columns: vec![
                    Column {
                        name: "id".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: true,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "floats".to_owned(),
                        col_type: ColumnType::Array(RcOrArc::new(ColumnType::Float)),
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "doubles".to_owned(),
                        col_type: ColumnType::Array(RcOrArc::new(ColumnType::Double)),
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                ],
                relations: vec![],
                conjunct_relations: vec![],
                primary_keys: vec![PrimaryKey {
                    name: "id".to_owned(),
                }],
            },
            Entity {
                table_name: "parent".to_owned(),
                columns: vec![
                    Column {
                        name: "id1".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "id2".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                ],
                relations: vec![Relation {
                    ref_table: "child".to_owned(),
                    columns: vec![],
                    ref_columns: vec![],
                    rel_type: RelationType::HasMany,
                    on_delete: None,
                    on_update: None,
                    self_referencing: false,
                    num_suffix: 0,
                    impl_related: true,
                }],
                conjunct_relations: vec![],
                primary_keys: vec![
                    PrimaryKey {
                        name: "id1".to_owned(),
                    },
                    PrimaryKey {
                        name: "id2".to_owned(),
                    },
                ],
            },
            Entity {
                table_name: "child".to_owned(),
                columns: vec![
                    Column {
                        name: "id".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: true,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "parent_id1".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "parent_id2".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                ],
                relations: vec![Relation {
                    ref_table: "parent".to_owned(),
                    columns: vec!["parent_id1".to_owned(), "parent_id2".to_owned()],
                    ref_columns: vec!["id1".to_owned(), "id2".to_owned()],
                    rel_type: RelationType::BelongsTo,
                    on_delete: None,
                    on_update: None,
                    self_referencing: false,
                    num_suffix: 0,
                    impl_related: true,
                }],
                conjunct_relations: vec![],
                primary_keys: vec![PrimaryKey {
                    name: "id".to_owned(),
                }],
            },
        ]
    }

    fn parse_from_file<R>(inner: R) -> io::Result<TokenStream>
    where
        R: Read,
    {
        let mut reader = BufReader::new(inner);
        let mut lines: Vec<String> = Vec::new();

        reader.read_until(b';', &mut Vec::new())?;

        let mut line = String::new();
        while reader.read_line(&mut line)? > 0 {
            lines.push(line.to_owned());
            line.clear();
        }
        let content = lines.join("");
        Ok(content.parse().unwrap())
    }

    #[test]
    fn test_gen_expanded_code_blocks() -> io::Result<()> {
        let entities = setup();
        const ENTITY_FILES: [&str; 13] = [
            include_str!("../../tests/expanded/cake.rs"),
            include_str!("../../tests/expanded/cake_filling.rs"),
            include_str!("../../tests/expanded/cake_filling_price.rs"),
            include_str!("../../tests/expanded/filling.rs"),
            include_str!("../../tests/expanded/fruit.rs"),
            include_str!("../../tests/expanded/vendor.rs"),
            include_str!("../../tests/expanded/rust_keyword.rs"),
            include_str!("../../tests/expanded/cake_with_float.rs"),
            include_str!("../../tests/expanded/cake_with_double.rs"),
            include_str!("../../tests/expanded/collection.rs"),
            include_str!("../../tests/expanded/collection_float.rs"),
            include_str!("../../tests/expanded/parent.rs"),
            include_str!("../../tests/expanded/child.rs"),
        ];
        const ENTITY_FILES_WITH_SCHEMA_NAME: [&str; 13] = [
            include_str!("../../tests/expanded_with_schema_name/cake.rs"),
            include_str!("../../tests/expanded_with_schema_name/cake_filling.rs"),
            include_str!("../../tests/expanded_with_schema_name/cake_filling_price.rs"),
            include_str!("../../tests/expanded_with_schema_name/filling.rs"),
            include_str!("../../tests/expanded_with_schema_name/fruit.rs"),
            include_str!("../../tests/expanded_with_schema_name/vendor.rs"),
            include_str!("../../tests/expanded_with_schema_name/rust_keyword.rs"),
            include_str!("../../tests/expanded_with_schema_name/cake_with_float.rs"),
            include_str!("../../tests/expanded_with_schema_name/cake_with_double.rs"),
            include_str!("../../tests/expanded_with_schema_name/collection.rs"),
            include_str!("../../tests/expanded_with_schema_name/collection_float.rs"),
            include_str!("../../tests/expanded_with_schema_name/parent.rs"),
            include_str!("../../tests/expanded_with_schema_name/child.rs"),
        ];

        assert_eq!(entities.len(), ENTITY_FILES.len());

        for (i, entity) in entities.iter().enumerate() {
            assert_eq!(
                parse_from_file(ENTITY_FILES[i].as_bytes())?.to_string(),
                EntityWriter::gen_expanded_code_blocks(
                    entity,
                    &crate::WithSerde::None,
                    &crate::DateTimeCrate::Chrono,
                    &None,
                    false,
                    false,
                    &TokenStream::new(),
                    &TokenStream::new(),
                    false,
                    true,
                )
                .into_iter()
                .skip(1)
                .fold(TokenStream::new(), |mut acc, tok| {
                    acc.extend(tok);
                    acc
                })
                .to_string()
            );
            assert_eq!(
                parse_from_file(ENTITY_FILES_WITH_SCHEMA_NAME[i].as_bytes())?.to_string(),
                EntityWriter::gen_expanded_code_blocks(
                    entity,
                    &crate::WithSerde::None,
                    &crate::DateTimeCrate::Chrono,
                    &Some("schema_name".to_owned()),
                    false,
                    false,
                    &TokenStream::new(),
                    &TokenStream::new(),
                    false,
                    true,
                )
                .into_iter()
                .skip(1)
                .fold(TokenStream::new(), |mut acc, tok| {
                    acc.extend(tok);
                    acc
                })
                .to_string()
            );
        }

        Ok(())
    }

    #[test]
    fn test_gen_compact_code_blocks() -> io::Result<()> {
        let entities = setup();
        const ENTITY_FILES: [&str; 13] = [
            include_str!("../../tests/compact/cake.rs"),
            include_str!("../../tests/compact/cake_filling.rs"),
            include_str!("../../tests/compact/cake_filling_price.rs"),
            include_str!("../../tests/compact/filling.rs"),
            include_str!("../../tests/compact/fruit.rs"),
            include_str!("../../tests/compact/vendor.rs"),
            include_str!("../../tests/compact/rust_keyword.rs"),
            include_str!("../../tests/compact/cake_with_float.rs"),
            include_str!("../../tests/compact/cake_with_double.rs"),
            include_str!("../../tests/compact/collection.rs"),
            include_str!("../../tests/compact/collection_float.rs"),
            include_str!("../../tests/compact/parent.rs"),
            include_str!("../../tests/compact/child.rs"),
        ];
        const ENTITY_FILES_WITH_SCHEMA_NAME: [&str; 13] = [
            include_str!("../../tests/compact_with_schema_name/cake.rs"),
            include_str!("../../tests/compact_with_schema_name/cake_filling.rs"),
            include_str!("../../tests/compact_with_schema_name/cake_filling_price.rs"),
            include_str!("../../tests/compact_with_schema_name/filling.rs"),
            include_str!("../../tests/compact_with_schema_name/fruit.rs"),
            include_str!("../../tests/compact_with_schema_name/vendor.rs"),
            include_str!("../../tests/compact_with_schema_name/rust_keyword.rs"),
            include_str!("../../tests/compact_with_schema_name/cake_with_float.rs"),
            include_str!("../../tests/compact_with_schema_name/cake_with_double.rs"),
            include_str!("../../tests/compact_with_schema_name/collection.rs"),
            include_str!("../../tests/compact_with_schema_name/collection_float.rs"),
            include_str!("../../tests/compact_with_schema_name/parent.rs"),
            include_str!("../../tests/compact_with_schema_name/child.rs"),
        ];

        assert_eq!(entities.len(), ENTITY_FILES.len());

        for (i, entity) in entities.iter().enumerate() {
            assert_eq!(
                parse_from_file(ENTITY_FILES[i].as_bytes())?.to_string(),
                EntityWriter::gen_compact_code_blocks(
                    entity,
                    &crate::WithSerde::None,
                    &crate::DateTimeCrate::Chrono,
                    &None,
                    false,
                    false,
                    &TokenStream::new(),
                    &TokenStream::new(),
                    false,
                    true,
                )
                .into_iter()
                .skip(1)
                .fold(TokenStream::new(), |mut acc, tok| {
                    acc.extend(tok);
                    acc
                })
                .to_string()
            );
            assert_eq!(
                parse_from_file(ENTITY_FILES_WITH_SCHEMA_NAME[i].as_bytes())?.to_string(),
                EntityWriter::gen_compact_code_blocks(
                    entity,
                    &crate::WithSerde::None,
                    &crate::DateTimeCrate::Chrono,
                    &Some("schema_name".to_owned()),
                    false,
                    false,
                    &TokenStream::new(),
                    &TokenStream::new(),
                    false,
                    true,
                )
                .into_iter()
                .skip(1)
                .fold(TokenStream::new(), |mut acc, tok| {
                    acc.extend(tok);
                    acc
                })
                .to_string()
            );
        }

        Ok(())
    }

    #[test]
    fn test_gen_with_serde() -> io::Result<()> {
        let cake_entity = setup().get(0).unwrap().clone();

        assert_eq!(cake_entity.get_table_name_snake_case(), "cake");

        // Compact code blocks
        assert_eq!(
            comparable_file_string(include_str!("../../tests/compact_with_serde/cake_none.rs"))?,
            generated_to_string(EntityWriter::gen_compact_code_blocks(
                &cake_entity,
                &WithSerde::None,
                &DateTimeCrate::Chrono,
                &None,
                false,
                false,
                &TokenStream::new(),
                &TokenStream::new(),
                false,
                true,
            ))
        );
        assert_eq!(
            comparable_file_string(include_str!(
                "../../tests/compact_with_serde/cake_serialize.rs"
            ))?,
            generated_to_string(EntityWriter::gen_compact_code_blocks(
                &cake_entity,
                &WithSerde::Serialize,
                &DateTimeCrate::Chrono,
                &None,
                false,
                false,
                &TokenStream::new(),
                &TokenStream::new(),
                false,
                true,
            ))
        );
        assert_eq!(
            comparable_file_string(include_str!(
                "../../tests/compact_with_serde/cake_deserialize.rs"
            ))?,
            generated_to_string(EntityWriter::gen_compact_code_blocks(
                &cake_entity,
                &WithSerde::Deserialize,
                &DateTimeCrate::Chrono,
                &None,
                true,
                false,
                &TokenStream::new(),
                &TokenStream::new(),
                false,
                true,
            ))
        );
        assert_eq!(
            comparable_file_string(include_str!("../../tests/compact_with_serde/cake_both.rs"))?,
            generated_to_string(EntityWriter::gen_compact_code_blocks(
                &cake_entity,
                &WithSerde::Both,
                &DateTimeCrate::Chrono,
                &None,
                true,
                false,
                &TokenStream::new(),
                &TokenStream::new(),
                false,
                true,
            ))
        );

        // Expanded code blocks
        assert_eq!(
            comparable_file_string(include_str!("../../tests/expanded_with_serde/cake_none.rs"))?,
            generated_to_string(EntityWriter::gen_expanded_code_blocks(
                &cake_entity,
                &WithSerde::None,
                &DateTimeCrate::Chrono,
                &None,
                false,
                false,
                &TokenStream::new(),
                &TokenStream::new(),
                false,
                true,
            ))
        );
        assert_eq!(
            comparable_file_string(include_str!(
                "../../tests/expanded_with_serde/cake_serialize.rs"
            ))?,
            generated_to_string(EntityWriter::gen_expanded_code_blocks(
                &cake_entity,
                &WithSerde::Serialize,
                &DateTimeCrate::Chrono,
                &None,
                false,
                false,
                &TokenStream::new(),
                &TokenStream::new(),
                false,
                true,
            ))
        );
        assert_eq!(
            comparable_file_string(include_str!(
                "../../tests/expanded_with_serde/cake_deserialize.rs"
            ))?,
            generated_to_string(EntityWriter::gen_expanded_code_blocks(
                &cake_entity,
                &WithSerde::Deserialize,
                &DateTimeCrate::Chrono,
                &None,
                true,
                false,
                &TokenStream::new(),
                &TokenStream::new(),
                false,
                true,
            ))
        );
        assert_eq!(
            comparable_file_string(include_str!("../../tests/expanded_with_serde/cake_both.rs"))?,
            generated_to_string(EntityWriter::gen_expanded_code_blocks(
                &cake_entity,
                &WithSerde::Both,
                &DateTimeCrate::Chrono,
                &None,
                true,
                false,
                &TokenStream::new(),
                &TokenStream::new(),
                false,
                true,
            ))
        );

        Ok(())
    }

    #[test]
    fn test_gen_with_seaography() -> io::Result<()> {
        let cake_entity = Entity {
            table_name: "cake".to_owned(),
            columns: vec![
                Column {
                    name: "id".to_owned(),
                    col_type: ColumnType::Integer,
                    auto_increment: true,
                    not_null: true,
                    unique: false,
                },
                Column {
                    name: "name".to_owned(),
                    col_type: ColumnType::Text,
                    auto_increment: false,
                    not_null: false,
                    unique: false,
                },
                Column {
                    name: "base_id".to_owned(),
                    col_type: ColumnType::Integer,
                    auto_increment: false,
                    not_null: false,
                    unique: false,
                },
            ],
            relations: vec![
                Relation {
                    ref_table: "fruit".to_owned(),
                    columns: vec![],
                    ref_columns: vec![],
                    rel_type: RelationType::HasMany,
                    on_delete: None,
                    on_update: None,
                    self_referencing: false,
                    num_suffix: 0,
                    impl_related: true,
                },
                Relation {
                    ref_table: "cake".to_owned(),
                    columns: vec![],
                    ref_columns: vec![],
                    rel_type: RelationType::HasOne,
                    on_delete: None,
                    on_update: None,
                    self_referencing: true,
                    num_suffix: 0,
                    impl_related: true,
                },
            ],
            conjunct_relations: vec![ConjunctRelation {
                via: "cake_filling".to_owned(),
                to: "filling".to_owned(),
            }],
            primary_keys: vec![PrimaryKey {
                name: "id".to_owned(),
            }],
        };

        assert_eq!(cake_entity.get_table_name_snake_case(), "cake");

        // Compact code blocks
        assert_eq!(
            comparable_file_string(include_str!("../../tests/with_seaography/cake.rs"))?,
            generated_to_string(EntityWriter::gen_compact_code_blocks(
                &cake_entity,
                &WithSerde::None,
                &DateTimeCrate::Chrono,
                &None,
                false,
                false,
                &TokenStream::new(),
                &TokenStream::new(),
                true,
                true,
            ))
        );

        // Expanded code blocks
        assert_eq!(
            comparable_file_string(include_str!("../../tests/with_seaography/cake_expanded.rs"))?,
            generated_to_string(EntityWriter::gen_expanded_code_blocks(
                &cake_entity,
                &WithSerde::None,
                &DateTimeCrate::Chrono,
                &None,
                false,
                false,
                &TokenStream::new(),
                &TokenStream::new(),
                true,
                true,
            ))
        );

        Ok(())
    }

    #[test]
    fn test_gen_with_seaography_mod() -> io::Result<()> {
        use crate::ActiveEnum;
        use sea_query::IntoIden;

        let entities = setup();
        let enums = vec![
            (
                "coinflip_result_type",
                ActiveEnum {
                    enum_name: Alias::new("coinflip_result_type").into_iden(),
                    values: vec!["HEADS", "TAILS"]
                        .into_iter()
                        .map(|variant| Alias::new(variant).into_iden())
                        .collect(),
                },
            ),
            (
                "media_type",
                ActiveEnum {
                    enum_name: Alias::new("media_type").into_iden(),
                    values: vec![
                        "UNKNOWN",
                        "BITMAP",
                        "DRAWING",
                        "AUDIO",
                        "VIDEO",
                        "MULTIMEDIA",
                        "OFFICE",
                        "TEXT",
                        "EXECUTABLE",
                        "ARCHIVE",
                        "3D",
                    ]
                    .into_iter()
                    .map(|variant| Alias::new(variant).into_iden())
                    .collect(),
                },
            ),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        assert_eq!(
            comparable_file_string(include_str!("../../tests/with_seaography/mod.rs"))?,
            generated_to_string(vec![EntityWriter::gen_seaography_entity_mod(
                &entities, &enums,
            )])
        );

        Ok(())
    }

    #[test]
    fn test_gen_with_derives() -> io::Result<()> {
        let mut cake_entity = setup().get_mut(0).unwrap().clone();

        assert_eq!(cake_entity.get_table_name_snake_case(), "cake");

        // Compact code blocks
        assert_eq!(
            comparable_file_string(include_str!(
                "../../tests/compact_with_derives/cake_none.rs"
            ))?,
            generated_to_string(EntityWriter::gen_compact_code_blocks(
                &cake_entity,
                &WithSerde::None,
                &DateTimeCrate::Chrono,
                &None,
                false,
                false,
                &TokenStream::new(),
                &TokenStream::new(),
                false,
                true,
            ))
        );
        assert_eq!(
            comparable_file_string(include_str!("../../tests/compact_with_derives/cake_one.rs"))?,
            generated_to_string(EntityWriter::gen_compact_code_blocks(
                &cake_entity,
                &WithSerde::None,
                &DateTimeCrate::Chrono,
                &None,
                false,
                false,
                &bonus_derive(["ts_rs::TS"]),
                &TokenStream::new(),
                false,
                true,
            ))
        );
        assert_eq!(
            comparable_file_string(include_str!(
                "../../tests/compact_with_derives/cake_multiple.rs"
            ))?,
            generated_to_string(EntityWriter::gen_compact_code_blocks(
                &cake_entity,
                &WithSerde::None,
                &DateTimeCrate::Chrono,
                &None,
                false,
                false,
                &bonus_derive(["ts_rs::TS", "utoipa::ToSchema"]),
                &TokenStream::new(),
                false,
                true,
            ))
        );

        // Expanded code blocks
        assert_eq!(
            comparable_file_string(include_str!(
                "../../tests/expanded_with_derives/cake_none.rs"
            ))?,
            generated_to_string(EntityWriter::gen_expanded_code_blocks(
                &cake_entity,
                &WithSerde::None,
                &DateTimeCrate::Chrono,
                &None,
                false,
                false,
                &TokenStream::new(),
                &TokenStream::new(),
                false,
                true,
            ))
        );
        assert_eq!(
            comparable_file_string(include_str!(
                "../../tests/expanded_with_derives/cake_one.rs"
            ))?,
            generated_to_string(EntityWriter::gen_expanded_code_blocks(
                &cake_entity,
                &WithSerde::None,
                &DateTimeCrate::Chrono,
                &None,
                false,
                false,
                &bonus_derive(["ts_rs::TS"]),
                &TokenStream::new(),
                false,
                true,
            ))
        );
        assert_eq!(
            comparable_file_string(include_str!(
                "../../tests/expanded_with_derives/cake_multiple.rs"
            ))?,
            generated_to_string(EntityWriter::gen_expanded_code_blocks(
                &cake_entity,
                &WithSerde::None,
                &DateTimeCrate::Chrono,
                &None,
                false,
                false,
                &bonus_derive(["ts_rs::TS", "utoipa::ToSchema"]),
                &TokenStream::new(),
                false,
                true,
            ))
        );

        // Make the `name` column of `cake` entity as hidden column
        cake_entity.columns[1].name = "_name".into();

        assert_serde_variant_results(
            &cake_entity,
            &(
                include_str!("../../tests/compact_with_serde/cake_serialize_with_hidden_column.rs"),
                WithSerde::Serialize,
                None,
            ),
            Box::new(EntityWriter::gen_compact_code_blocks),
        )?;
        assert_serde_variant_results(
            &cake_entity,
            &(
                include_str!(
                    "../../tests/expanded_with_serde/cake_serialize_with_hidden_column.rs"
                ),
                WithSerde::Serialize,
                None,
            ),
            Box::new(EntityWriter::gen_expanded_code_blocks),
        )?;

        Ok(())
    }

    #[allow(clippy::type_complexity)]
    fn assert_serde_variant_results(
        cake_entity: &Entity,
        entity_serde_variant: &(&str, WithSerde, Option<String>),
        generator: Box<
            dyn Fn(
                &Entity,
                &WithSerde,
                &DateTimeCrate,
                &Option<String>,
                bool,
                bool,
                &TokenStream,
                &TokenStream,
                bool,
                bool,
            ) -> Vec<TokenStream>,
        >,
    ) -> io::Result<()> {
        let mut reader = BufReader::new(entity_serde_variant.0.as_bytes());
        let mut lines: Vec<String> = Vec::new();
        let serde_skip_deserializing_primary_key = matches!(
            entity_serde_variant.1,
            WithSerde::Both | WithSerde::Deserialize
        );
        let serde_skip_hidden_column = matches!(entity_serde_variant.1, WithSerde::Serialize);

        reader.read_until(b'\n', &mut Vec::new())?;

        let mut line = String::new();
        while reader.read_line(&mut line)? > 0 {
            lines.push(line.to_owned());
            line.clear();
        }
        let content = lines.join("");
        let expected: TokenStream = content.parse().unwrap();
        println!("{:?}", entity_serde_variant.1);
        let generated = generator(
            cake_entity,
            &entity_serde_variant.1,
            &DateTimeCrate::Chrono,
            &entity_serde_variant.2,
            serde_skip_deserializing_primary_key,
            serde_skip_hidden_column,
            &TokenStream::new(),
            &TokenStream::new(),
            false,
            true,
        )
        .into_iter()
        .fold(TokenStream::new(), |mut acc, tok| {
            acc.extend(tok);
            acc
        });

        assert_eq!(expected.to_string(), generated.to_string());
        Ok(())
    }

    #[test]
    fn test_gen_with_attributes() -> io::Result<()> {
        let cake_entity = setup().get(0).unwrap().clone();

        assert_eq!(cake_entity.get_table_name_snake_case(), "cake");

        // Compact code blocks
        assert_eq!(
            comparable_file_string(include_str!(
                "../../tests/compact_with_attributes/cake_none.rs"
            ))?,
            generated_to_string(EntityWriter::gen_compact_code_blocks(
                &cake_entity,
                &WithSerde::None,
                &DateTimeCrate::Chrono,
                &None,
                false,
                false,
                &TokenStream::new(),
                &TokenStream::new(),
                false,
                true,
            ))
        );
        assert_eq!(
            comparable_file_string(include_str!(
                "../../tests/compact_with_attributes/cake_one.rs"
            ))?,
            generated_to_string(EntityWriter::gen_compact_code_blocks(
                &cake_entity,
                &WithSerde::None,
                &DateTimeCrate::Chrono,
                &None,
                false,
                false,
                &TokenStream::new(),
                &bonus_attributes([r#"serde(rename_all = "camelCase")"#]),
                false,
                true,
            ))
        );
        assert_eq!(
            comparable_file_string(include_str!(
                "../../tests/compact_with_attributes/cake_multiple.rs"
            ))?,
            generated_to_string(EntityWriter::gen_compact_code_blocks(
                &cake_entity,
                &WithSerde::None,
                &DateTimeCrate::Chrono,
                &None,
                false,
                false,
                &TokenStream::new(),
                &bonus_attributes([r#"serde(rename_all = "camelCase")"#, "ts(export)"]),
                false,
                true,
            ))
        );

        // Expanded code blocks
        assert_eq!(
            comparable_file_string(include_str!(
                "../../tests/expanded_with_attributes/cake_none.rs"
            ))?,
            generated_to_string(EntityWriter::gen_expanded_code_blocks(
                &cake_entity,
                &WithSerde::None,
                &DateTimeCrate::Chrono,
                &None,
                false,
                false,
                &TokenStream::new(),
                &TokenStream::new(),
                false,
                true,
            ))
        );
        assert_eq!(
            comparable_file_string(include_str!(
                "../../tests/expanded_with_attributes/cake_one.rs"
            ))?,
            generated_to_string(EntityWriter::gen_expanded_code_blocks(
                &cake_entity,
                &WithSerde::None,
                &DateTimeCrate::Chrono,
                &None,
                false,
                false,
                &TokenStream::new(),
                &bonus_attributes([r#"serde(rename_all = "camelCase")"#]),
                false,
                true,
            ))
        );
        assert_eq!(
            comparable_file_string(include_str!(
                "../../tests/expanded_with_attributes/cake_multiple.rs"
            ))?,
            generated_to_string(EntityWriter::gen_expanded_code_blocks(
                &cake_entity,
                &WithSerde::None,
                &DateTimeCrate::Chrono,
                &None,
                false,
                false,
                &TokenStream::new(),
                &bonus_attributes([r#"serde(rename_all = "camelCase")"#, "ts(export)"]),
                false,
                true,
            ))
        );

        Ok(())
    }

    fn generated_to_string(generated: Vec<TokenStream>) -> String {
        generated
            .into_iter()
            .fold(TokenStream::new(), |mut acc, tok| {
                acc.extend(tok);
                acc
            })
            .to_string()
    }

    fn comparable_file_string(file: &str) -> io::Result<String> {
        let mut reader = BufReader::new(file.as_bytes());
        let mut lines: Vec<String> = Vec::new();

        reader.read_until(b'\n', &mut Vec::new())?;

        let mut line = String::new();
        while reader.read_line(&mut line)? > 0 {
            lines.push(line.to_owned());
            line.clear();
        }
        let content = lines.join("");
        let expected: TokenStream = content.parse().unwrap();

        Ok(expected.to_string())
    }

    #[test]
    fn test_gen_postgres() -> io::Result<()> {
        let entities = vec![
            // This tests that the JsonBinary column type is annotated
            // correctly in compact entity form. More information can be found
            // in this issue:
            //
            // https://github.com/SeaQL/sea-orm/issues/1344
            Entity {
                table_name: "task".to_owned(),
                columns: vec![
                    Column {
                        name: "id".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: true,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "payload".to_owned(),
                        col_type: ColumnType::Json,
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "payload_binary".to_owned(),
                        col_type: ColumnType::JsonBinary,
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                ],
                relations: vec![],
                conjunct_relations: vec![],
                primary_keys: vec![PrimaryKey {
                    name: "id".to_owned(),
                }],
            },
        ];
        const ENTITY_FILES: [&str; 1] = [include_str!("../../tests/postgres/binary_json.rs")];

        const ENTITY_FILES_EXPANDED: [&str; 1] =
            [include_str!("../../tests/postgres/binary_json_expanded.rs")];

        assert_eq!(entities.len(), ENTITY_FILES.len());

        for (i, entity) in entities.iter().enumerate() {
            assert_eq!(
                parse_from_file(ENTITY_FILES[i].as_bytes())?.to_string(),
                EntityWriter::gen_compact_code_blocks(
                    entity,
                    &crate::WithSerde::None,
                    &crate::DateTimeCrate::Chrono,
                    &None,
                    false,
                    false,
                    &TokenStream::new(),
                    &TokenStream::new(),
                    false,
                    true,
                )
                .into_iter()
                .skip(1)
                .fold(TokenStream::new(), |mut acc, tok| {
                    acc.extend(tok);
                    acc
                })
                .to_string()
            );
            assert_eq!(
                parse_from_file(ENTITY_FILES_EXPANDED[i].as_bytes())?.to_string(),
                EntityWriter::gen_expanded_code_blocks(
                    entity,
                    &crate::WithSerde::None,
                    &crate::DateTimeCrate::Chrono,
                    &Some("schema_name".to_owned()),
                    false,
                    false,
                    &TokenStream::new(),
                    &TokenStream::new(),
                    false,
                    true,
                )
                .into_iter()
                .skip(1)
                .fold(TokenStream::new(), |mut acc, tok| {
                    acc.extend(tok);
                    acc
                })
                .to_string()
            );
        }

        Ok(())
    }

    #[test]
    fn test_gen_import_active_enum() -> io::Result<()> {
        let entities = vec![
            Entity {
                table_name: "tea_pairing".to_owned(),
                columns: vec![
                    Column {
                        name: "id".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: true,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "first_tea".to_owned(),
                        col_type: ColumnType::Enum {
                            name: SeaRc::new(Alias::new("tea_enum")),
                            variants: vec![
                                SeaRc::new(Alias::new("everyday_tea")),
                                SeaRc::new(Alias::new("breakfast_tea")),
                            ],
                        },
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "second_tea".to_owned(),
                        col_type: ColumnType::Enum {
                            name: SeaRc::new(Alias::new("tea_enum")),
                            variants: vec![
                                SeaRc::new(Alias::new("everyday_tea")),
                                SeaRc::new(Alias::new("breakfast_tea")),
                            ],
                        },
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                ],
                relations: vec![],
                conjunct_relations: vec![],
                primary_keys: vec![PrimaryKey {
                    name: "id".to_owned(),
                }],
            },
            Entity {
                table_name: "tea_pairing_with_size".to_owned(),
                columns: vec![
                    Column {
                        name: "id".to_owned(),
                        col_type: ColumnType::Integer,
                        auto_increment: true,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "first_tea".to_owned(),
                        col_type: ColumnType::Enum {
                            name: SeaRc::new(Alias::new("tea_enum")),
                            variants: vec![
                                SeaRc::new(Alias::new("everyday_tea")),
                                SeaRc::new(Alias::new("breakfast_tea")),
                            ],
                        },
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "second_tea".to_owned(),
                        col_type: ColumnType::Enum {
                            name: SeaRc::new(Alias::new("tea_enum")),
                            variants: vec![
                                SeaRc::new(Alias::new("everyday_tea")),
                                SeaRc::new(Alias::new("breakfast_tea")),
                            ],
                        },
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                    Column {
                        name: "size".to_owned(),
                        col_type: ColumnType::Enum {
                            name: SeaRc::new(Alias::new("tea_size")),
                            variants: vec![
                                SeaRc::new(Alias::new("small")),
                                SeaRc::new(Alias::new("medium")),
                                SeaRc::new(Alias::new("huge")),
                            ],
                        },
                        auto_increment: false,
                        not_null: true,
                        unique: false,
                    },
                ],
                relations: vec![],
                conjunct_relations: vec![],
                primary_keys: vec![PrimaryKey {
                    name: "id".to_owned(),
                }],
            },
        ];

        assert_eq!(
            quote!(
                use super::sea_orm_active_enums::TeaEnum;
            )
            .to_string(),
            EntityWriter::gen_import_active_enum(&entities[0]).to_string()
        );

        assert_eq!(
            quote!(
                use super::sea_orm_active_enums::TeaEnum;
                use super::sea_orm_active_enums::TeaSize;
            )
            .to_string(),
            EntityWriter::gen_import_active_enum(&entities[1]).to_string()
        );

        Ok(())
    }
}
