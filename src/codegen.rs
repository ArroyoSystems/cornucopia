use std::collections::HashMap;

use error::Error;
use heck::ToUpperCamelCase;

use crate::{
    parse::Quantifier,
    pg_type::{CornucopiaType, CornucopiaTypeKind, TypeRegistrar},
    prepare_queries::{PreparedQuery, RustReturnType},
};

use self::error::WriteFileError;

use super::prepare_queries::PreparedModule;

pub(crate) fn generate_query(module_name: &str, query: &PreparedQuery) -> Result<String, Error> {
    let name = &query.name;
    let query_struct = generate_query_struct(query)?.unwrap_or_default();
    let params = generate_query_params(query)?;
    let ret_ty = generate_query_ret_ty(query)?;
    let ret_ty = if let RustReturnType::Struct(_) = query.ret {
        format!("super::super::queries::{module_name}::{ret_ty}")
    } else {
        ret_ty
    };

    let ret = generate_query_quantified_ret_ty(query, &ret_ty);
    let body = generate_query_body(query, ret_ty)?;
    let query_string = format!(
        "{query_struct}
    pub async fn {name}<T: GenericClient>(client:&T, {params}) -> Result<{ret},Error> {{{body}}}"
    );

    Ok(query_string)
}

pub(crate) fn generate_custom_type(ty: &CornucopiaType) -> Result<String, Error> {
    let type_def = match &ty.kind {
        CornucopiaTypeKind::Enum(variants) => {
            let name = &ty.rust_ty_name;
            format!(
                "#[derive(Clone, Copy, PartialEq, Eq)]\npub enum {} {{ {} }}",
                name,
                variants.join(",")
            )
        }
        CornucopiaTypeKind::Domain(domain_inner_ty) => {
            let name = &domain_inner_ty.rust_ty_name;
            format!(
                "#[derive(Clone)]\npub struct {} ({})",
                ty.pg_ty.schema(),
                name
            )
        }
        CornucopiaTypeKind::Composite(fields) => {
            let fields_str = fields
                .iter()
                .map(|f| format!("pub {} : {}", f.name, f.ty.rust_ty_definition_path))
                .collect::<Vec<String>>()
                .join(",");

            let name = &ty.rust_ty_name;
            format!("#[derive(Clone)]\npub struct {} {{ {} }}", name, fields_str)
        }
        _ => return Err(Error::NonBaseType),
    };

    Ok(format!(
        "#[derive(Debug, ToSql, FromSql)]\n#[postgres(name = \"{}\")]\n{}",
        ty.pg_ty.name(),
        type_def
    ))
}

pub(crate) fn generate_type_modules(type_registrar: &TypeRegistrar) -> Result<String, Error> {
    let mut modules = HashMap::<String, Vec<CornucopiaType>>::new();
    for ((schema, _), ty) in &type_registrar.custom_types {
        match modules.entry(schema.to_owned()) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.get_mut().push(ty.clone());
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(vec![ty.clone()]);
            }
        }
    }
    let schema_modules = modules
        .iter()
        .map(|(mod_name, tys)| {
            let tys_str = tys
                .iter()
                .map(generate_custom_type)
                .collect::<Result<Vec<String>, Error>>()?
                .join("\n\n");
            Ok(format!(
                "pub mod {mod_name} {{ use postgres_types::{{FromSql, ToSql}}; {tys_str} }}"
            ))
        })
        .collect::<Result<Vec<String>, Error>>()?
        .join("\n\n");

    Ok(format!("pub mod types {{ {schema_modules} }}"))
}

pub(crate) fn generate_query_struct(query: &PreparedQuery) -> Result<Option<String>, Error> {
    if let RustReturnType::Struct(fields) = &query.ret {
        let mut field_strings = Vec::new();
        for (param, param_ty) in fields {
            let ty_string = if param.is_nullable {
                format!("Option<{}>", param_ty.rust_ty_usage_path)
            } else {
                param_ty.rust_ty_usage_path.clone()
            };
            field_strings.push(format!("pub {} : {}", param.name, ty_string));
        }
        let fields_string = field_strings.join(",");
        let struct_name = query.name.to_upper_camel_case();

        Ok(Some(format!(
            "#[derive(Debug, Clone, PartialEq)] pub struct {struct_name} {{{fields_string}}}"
        )))
    } else {
        Ok(None)
    }
}

pub(crate) fn generate_query_quantified_ret_ty(query: &PreparedQuery, ret_ty: &str) -> String {
    if let RustReturnType::Void = query.ret {
        String::from("()")
    } else {
        match query.quantifier {
            Quantifier::ZeroOrMore => format!("Vec<{ret_ty}>"),
            Quantifier::ZeroOrOne => format!("Option<{ret_ty}>"),
            _ => ret_ty.to_string(),
        }
    }
}

pub(crate) fn generate_query_params(query: &PreparedQuery) -> Result<String, Error> {
    let mut param_strings = Vec::new();
    for param in &query.params {
        let rs_ty_borrowed = &param.ty.borrowed_rust_ty();
        let param_string = format!("{} : {}", param.name, rs_ty_borrowed);
        param_strings.push(param_string);
    }
    Ok(param_strings.join(","))
}

pub(crate) fn generate_query_ret_ty(query: &PreparedQuery) -> Result<String, Error> {
    Ok(match &query.ret {
        RustReturnType::Void => String::from("()"),
        RustReturnType::Scalar(ty) => ty.rust_ty_usage_path.clone(),
        RustReturnType::Tuple(tys) => {
            let mut tys_string = Vec::new();
            for ty in tys {
                tys_string.push(ty.rust_ty_usage_path.clone())
            }
            format!("({})", tys_string.join(","))
        }
        RustReturnType::Struct(_) => query.name.to_upper_camel_case(),
        RustReturnType::Raw => String::from("tokio_postgres::RowStream"),
    })
}

pub(crate) fn generate_query_body(query: &PreparedQuery, ret_ty: String) -> Result<String, Error> {
    let query_string = format!(r#""{}""#, &query.sql);

    let query_method = if let RustReturnType::Void = query.ret {
        "execute"
    } else {
        match query.quantifier {
            Quantifier::ZeroOrMore => "query",
            Quantifier::ZeroOrOne => "query_opt",
            Quantifier::One => "query_one",
            Quantifier::Raw => "query_raw",
        }
    };

    let query_param_values = query
        .params
        .iter()
        .map(|field| format!("&{}", field.name))
        .collect::<Vec<String>>()
        .join(",");

    let ret_value = match &query.ret {
        RustReturnType::Void => String::from("Ok(())"),
        RustReturnType::Scalar(_) => match query.quantifier {
            Quantifier::ZeroOrMore => {
                format!("let return_value = res.iter().map(|row| {{let value : {ret_ty} = row.get(0); value}}).collect::<Vec<{ret_ty}>>(); Ok(return_value)")
            }
            Quantifier::ZeroOrOne => {
                format!("let return_value = res.map(|row| {{let value: {ret_ty} = row.get(0); value}}); Ok(return_value)")
            }
            Quantifier::One => {
                format!("let return_value: {ret_ty} = res.get(0); Ok(return_value)")
            }
            Quantifier::Raw => return Err(Error::UnexpectedRawQuantifier),
        },
        RustReturnType::Tuple(tup) => {
            let mut rust_ret_values = Vec::new();
            let mut rust_ret_value_names = Vec::new();
            for (i, ty) in tup.iter().enumerate() {
                let rust_ret_value_name = format!("return_value_{}", i);
                rust_ret_values.push(format!(
                    "let {}: {} = res.get({});",
                    rust_ret_value_name, ty.rust_ty_usage_path, i
                ));
                rust_ret_value_names.push(rust_ret_value_name);
            }
            let tuple_value_string = format!(
                "{} ({})",
                rust_ret_values.join(" "),
                rust_ret_value_names.join(",")
            );
            match query.quantifier {
                Quantifier::ZeroOrMore => {
                    format!("let return_value = res.iter().map(|res| {{ {tuple_value_string} }}).collect::<Vec<{ret_ty}>>(); Ok(return_value)")
                }
                Quantifier::ZeroOrOne => {
                    format!("let return_value = res.map(|res| {{ {tuple_value_string} }}); Ok(return_value)")
                }
                Quantifier::One => {
                    format!("let return_value={{ {tuple_value_string} }}; Ok(return_value)")
                }
                Quantifier::Raw => return Err(Error::UnexpectedRawQuantifier),
            }
        }
        RustReturnType::Struct(structure) => {
            let mut field_values = Vec::new();
            let mut rust_ret_values = Vec::new();
            for (i, (param, param_ty)) in structure.iter().enumerate() {
                let ty_string = if param.is_nullable {
                    format!("Option<{}>", param_ty.rust_ty_usage_path)
                } else {
                    param_ty.rust_ty_usage_path.clone()
                };
                let rust_ret_value_name = format!("return_value_{}", i);
                rust_ret_values.push(format!(
                    "let {}: {} = res.get({});",
                    rust_ret_value_name, ty_string, i
                ));
                field_values.push(format!("{} : {}", param.name, rust_ret_value_name));
            }

            let struct_value_string = format!(
                "{} {} {{ {} }}",
                rust_ret_values.join(" "),
                ret_ty,
                field_values.join(",")
            );

            match query.quantifier {
                Quantifier::ZeroOrMore => {
                    format!("let return_value = res.iter().map(|res| {{ {struct_value_string} }}).collect::<Vec<{ret_ty}>>(); Ok(return_value)")
                }
                Quantifier::ZeroOrOne => {
                    format!("let return_value = res.map(|res| {{ {struct_value_string} }}); Ok(return_value)")
                }
                Quantifier::One => {
                    format!("let return_value={{ {struct_value_string} }}; Ok(return_value)")
                }
                Quantifier::Raw => return Err(Error::UnexpectedRawQuantifier),
            }
        }
        RustReturnType::Raw => String::from("Ok(res)"),
    };

    let res_var_name = match query.ret {
        RustReturnType::Void => "_",
        _ => "res",
    };
    let nb_params = query.params.len();
    let result_string = if let RustReturnType::Raw = query.ret {
        format!(
            "let params : [&dyn postgres_types::ToSql; {nb_params}] = [{query_param_values}];
let {res_var_name} = client.{query_method}(&stmt, params).await?;"
        )
    } else {
        format!(
            "let {res_var_name} = client.{query_method}(&stmt, &[{query_param_values}]).await?;"
        )
    };

    Ok(format!(
        "let stmt = client.prepare({query_string}).await?;
{result_string}
{ret_value}"
    ))
}

pub(crate) fn generate(
    type_registrar: &TypeRegistrar,
    modules: Vec<PreparedModule>,
    destination: &str,
) -> Result<(), Error> {
    let query_imports = "use cornucopia_client::GenericClient;\nuse tokio_postgres::Error;";
    let type_modules = generate_type_modules(type_registrar)?;

    let mut query_modules = Vec::new();
    for module in modules {
        let mut query_strings = Vec::new();
        for query in module.queries {
            let query_string = generate_query(&module.name, &query)?;
            query_strings.push(query_string);
        }
        let queries_string = query_strings.join("\n\n");
        let module_name = module.name;

        query_modules.push(format!(
            "pub mod {module_name} {{ {query_imports}\n{queries_string} }}"
        ));
    }
    let query_modules_string = format!("pub mod queries {{ {} }}", query_modules.join("\n\n"));
    let top_level_comment = "// This file was generated with `cornucopia`. Do not modify.";

    let generated_modules =
        format!("{top_level_comment}\n\n{type_modules}\n\n{query_modules_string}");

    std::fs::write(destination, generated_modules).map_err(|err| {
        Error::Io(WriteFileError {
            err,
            path: String::from(destination),
        })
    })?;

    Ok(())
}

pub(crate) mod error {
    use thiserror::Error as ThisError;

    #[derive(Debug, ThisError)]
    #[error("{0}")]
    pub(crate) enum Error {
        Io(#[from] WriteFileError),
        #[error("Got base or array type while generating custom types. This is a bug.")]
        NonBaseType,
        #[error("Got raw quantifier while generating non-raw return type. This is a bug.")]
        UnexpectedRawQuantifier,
    }

    #[derive(Debug, ThisError)]
    #[error("Error while trying to write to destination file \"{path}\": {err}.")]
    pub(crate) struct WriteFileError {
        pub(crate) err: std::io::Error,
        pub(crate) path: String,
    }
}
