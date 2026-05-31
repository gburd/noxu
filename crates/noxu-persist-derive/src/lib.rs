// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! > **Internal component of the [`noxu`](https://crates.io/crates/noxu) database.**
//! >
//! > This crate is published only so the `noxu` umbrella crate can depend on it.
//! > Use `noxu` (`noxu = "3"`) in applications; depend on this crate directly only
//! > if you are extending the engine internals. Its API may change without a major
//! > version bump.
//!
//! Derive macros for the Noxu DB Direct Persistence Layer (DPL).
//!
//! This crate provides three `#[derive(...)]` macros that generate the
//! boilerplate required to use a Rust struct with `noxu-persist`:
//!
//! - **`#[derive(Entity)]`** — implements `noxu_persist::Entity` for a
//!   user struct.  The struct must have exactly one field annotated with
//!   `#[primary_key]`; the field's type becomes
//!   `Entity::PrimaryKey`.  By default the entity name is the struct
//!   name; override with `#[entity(name = "...")]` at the struct level.
//!
//! - **`#[derive(PrimaryKey)]`** — implements
//!   `noxu_persist::PrimaryKey` for a user struct used as a *composite*
//!   or *newtype* primary key (analogue of `@KeyField` from BDB-JE).
//!   Supports tuple structs with one field (newtype) and named-field
//!   structs (composite, length-prefix concatenation).  The struct must
//!   also derive `Clone + PartialEq + Eq + Hash` for the `PrimaryKey`
//!   trait bounds; this is the user's responsibility.
//!
//! - **`#[derive(SecondaryKey)]`** — for each `#[secondary_key(...)]`
//!   field on the user struct, emits a typed
//!   `Foo::open_<name>_index(primary)` helper method that opens a
//!   `noxu_persist::SecondaryIndex` against a `PrimaryIndex`, plus a
//!   `pub const SECONDARY_INDEXES: &'static [SecondarySpec]` table
//!   describing every declared index.  This derive is independent of
//!   `derive(Entity)` (you can use either or both).
//!
//! # Why three macros?
//!
//! BDB-JE has three annotations: `@Entity`, `@PrimaryKey`,
//! `@SecondaryKey` — each addressing a different concern.  The Noxu
//! port preserves the same factoring so the porting guidance is
//! mechanical:
//!
//! | JE annotation | Noxu derive |
//! |---|---|
//! | `@Entity` | `#[derive(Entity)]` (struct level) |
//! | `@PrimaryKey` (field) | `#[primary_key]` (consumed by `Entity` derive) |
//! | `@PrimaryKeyField` / `@KeyField` | `#[derive(PrimaryKey)]` (struct level for composite key types) |
//! | `@SecondaryKey(name=…, relate=…, …)` | `#[secondary_key(name=…, relate=…, …)]` (field level, consumed by `SecondaryKey` derive) |
//!
//! # Crate-path escape hatch
//!
//! By default the generated code emits `::noxu::persist::…` paths, so
//! the `noxu` umbrella crate must be in the dependency graph.  Users who
//! depend on `noxu-persist` **directly** (without the umbrella) can
//! override this with the `crate` key inside the `#[entity(…)]`
//! container attribute:
//!
//! ```ignore
//! // Cargo.toml: noxu-persist = "3"  (no noxu umbrella needed)
//! use noxu_persist::{Entity, PrimaryKey, SecondaryKey};
//!
//! #[derive(Clone, PartialEq, Eq, Hash, PrimaryKey)]
//! #[entity(crate = "noxu_persist")]
//! struct UserId(u64);
//!
//! #[derive(Clone, Debug, Entity, SecondaryKey)]
//! #[entity(crate = "noxu_persist")]
//! struct User {
//!     #[primary_key]
//!     id: UserId,
//!     #[secondary_key(name = "by_email", relate = OneToOne)]
//!     email: String,
//! }
//! ```
//!
//! The `crate` key is accepted by all three derives via the
//! `#[entity(crate = "…")]` form.  A string literal containing a valid
//! Rust module path is required (validated at compile time); a malformed
//! path produces a descriptive compile error.
//!
//! # Re-exports
//!
//! The `noxu-persist` crate re-exports all three derives so users only
//! need a single dependency:
//!
//! ```ignore
//! use noxu_persist::{Entity, PrimaryKey, SecondaryKey};
//!
//! #[derive(Clone, Debug, Entity, SecondaryKey)]
//! #[entity(crate = "noxu_persist")]
//! struct User {
//!     #[primary_key]
//!     id: u64,
//!     #[secondary_key(name = "by_email", relate = OneToOne)]
//!     email: String,
//!     #[secondary_key(name = "by_dept", relate = ManyToOne, related_entity = "Department")]
//!     dept: Option<u64>,
//! }
//! ```

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::spanned::Spanned;
use syn::{
    Attribute, Data, DataStruct, DeriveInput, Expr, ExprLit, Field, Fields,
    GenericArgument, Lit, Meta, Path, PathArguments, Token, Type, TypePath,
    parse_macro_input,
};

// ============================================================================
// Crate-path helper
// ============================================================================

/// Parsed container-level `#[entity(…)]` attributes.
struct EntityContainerAttrs {
    /// Resolved entity name (either from `name = "…"` or struct ident).
    name: String,
    /// Crate root path for generated code (default: `::noxu::persist`).
    krate: Path,
}

/// Parse ALL recognised keys in `#[entity(…)]` in a single pass:
/// - `name = "…"` — entity name override
/// - `crate = "…"` — crate-root path override (escape hatch for direct
///   `noxu-persist` users; follows the `serde` `#[serde(crate = "…")]`
///   pattern)
///
/// Unknown keys produce a compile error listing both valid keys.
fn parse_entity_container_attrs(
    attrs: &[Attribute],
    fallback_ident: &syn::Ident,
) -> syn::Result<EntityContainerAttrs> {
    let mut name: Option<String> = None;
    let mut krate: Option<Path> = None;

    for attr in attrs {
        if !attr.path().is_ident("entity") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                let value = meta.value()?;
                let lit: syn::LitStr = value.parse()?;
                name = Some(lit.value());
                Ok(())
            } else if meta.path.is_ident("crate") {
                let value = meta.value()?;
                let lit: syn::LitStr = value.parse()?;
                let path = lit.parse_with(syn::Path::parse_mod_style).map_err(
                    |_| {
                        syn::Error::new(
                            lit.span(),
                            format!(
                                "`#[entity(crate = \"{v}\")]` is not a valid \
                                 Rust module path; expected e.g. \
                                 `\"noxu_persist\"` or `\"::noxu::persist\"`",
                                v = lit.value(),
                            ),
                        )
                    },
                )?;
                krate = Some(path);
                Ok(())
            } else {
                Err(meta.error(
                    "unrecognised attribute on `#[entity(...)]`; \
                     allowed keys: `name = \"...\"`, `crate = \"...\"`",
                ))
            }
        })?;
    }

    // Validate name is non-empty when explicitly set.
    if let Some(ref n) = name
        && n.is_empty()
    {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "`#[entity(name = \"\")]` is empty; entity names are \
             used as part of database names and must be non-empty",
        ));
    }

    let name = name.unwrap_or_else(|| fallback_ident.to_string());
    let krate = krate.unwrap_or_else(default_krate);

    Ok(EntityContainerAttrs { name, krate })
}

/// Parse ONLY the `crate = "…"` key from `#[entity(…)]`.
///
/// Used by `derive(PrimaryKey)` and the crate-root extraction path for
/// `derive(SecondaryKey)` when no full entity context is needed.
/// `name = "…"` is silently accepted (not required by these derives) so
/// that a struct carrying `#[entity(name = "…", crate = "…")]` compiles
/// without extra boilerplate.
fn parse_krate_from_entity_attr(attrs: &[Attribute]) -> syn::Result<Path> {
    let mut krate: Option<Path> = None;

    for attr in attrs {
        if !attr.path().is_ident("entity") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("crate") {
                let value = meta.value()?;
                let lit: syn::LitStr = value.parse()?;
                let path = lit.parse_with(syn::Path::parse_mod_style).map_err(
                    |_| {
                        syn::Error::new(
                            lit.span(),
                            format!(
                                "`#[entity(crate = \"{v}\")]` is not a valid \
                                 Rust module path; expected e.g. \
                                 `\"noxu_persist\"` or `\"::noxu::persist\"`",
                                v = lit.value(),
                            ),
                        )
                    },
                )?;
                krate = Some(path);
                Ok(())
            } else if meta.path.is_ident("name") {
                // `name` is a valid Entity attr; skip it silently here.
                let _: syn::LitStr = meta.value()?.parse()?;
                Ok(())
            } else {
                Err(meta.error(
                    "unrecognised attribute on `#[entity(...)]`; \
                     allowed keys: `name = \"...\"`, `crate = \"...\"`",
                ))
            }
        })?;
        if krate.is_some() {
            break;
        }
    }

    Ok(krate.unwrap_or_else(default_krate))
}

/// The default generated-code crate root: `::noxu::persist`.
///
/// This keeps zero-annotation users (those depending on the `noxu`
/// umbrella) working without any change.
fn default_krate() -> Path {
    syn::parse_str("::noxu::persist").expect("hardcoded default path is valid")
}

// ============================================================================
// #[derive(Entity)]
// ============================================================================

/// Derive `noxu_persist::Entity` for a struct.
///
/// See the [crate-level docs](crate) for the full attribute reference.
///
/// The struct must:
///
/// - have exactly one field annotated `#[primary_key]`,
/// - either have its struct name be the entity name, or carry a
///   `#[entity(name = "...")]` struct-level attribute.
///
/// Field types must implement `Clone`; the primary-key field type must
/// implement `noxu_persist::PrimaryKey`.
///
/// **Crate-path override**: add `#[entity(crate = "noxu_persist")]` to
/// direct the generated `impl` to use `::noxu_persist::…` instead of
/// `::noxu::persist::…`.  Required when depending on `noxu-persist`
/// without the `noxu` umbrella crate.
#[proc_macro_derive(Entity, attributes(entity, primary_key, secondary_key))]
pub fn derive_entity(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_entity(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand_entity(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let struct_ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) =
        input.generics.split_for_impl();

    let fields = struct_fields(input)?;
    let pk_field = find_primary_key_field(fields)?;
    let pk_ty = &pk_field.ty;
    let pk_ident = pk_field.ident.as_ref().ok_or_else(|| {
        syn::Error::new(
            pk_field.span(),
            "`#[primary_key]` is only supported on named fields; use \
             `#[derive(PrimaryKey)]` on a tuple struct to derive a key type",
        )
    })?;

    let EntityContainerAttrs { name: entity_name, krate } =
        parse_entity_container_attrs(&input.attrs, struct_ident)?;

    Ok(quote! {
        impl #impl_generics #krate::Entity for #struct_ident #ty_generics #where_clause {
            type PrimaryKey = #pk_ty;

            fn primary_key(&self) -> &#pk_ty {
                &self.#pk_ident
            }

            fn entity_name() -> &'static str {
                #entity_name
            }
        }
    })
}

// ============================================================================
// #[derive(PrimaryKey)]
// ============================================================================

/// Derive `noxu_persist::PrimaryKey` for a custom key struct.
///
/// Supported shapes:
///
/// 1. **Newtype** — `struct UserId(u64);` — delegates `to_bytes` /
///    `from_bytes` to the inner field's `PrimaryKey` impl.
///
/// 2. **Composite (named fields)** — each field is encoded by its
///    `PrimaryKey::to_bytes()` and length-prefixed (4-byte big-endian
///    `u32`) so decoding is unambiguous.  Field order in the struct
///    determines byte-lex sort order.
///
/// The user must separately derive `Clone + PartialEq + Eq + Hash` to
/// satisfy the `PrimaryKey` trait bounds; the macro does not emit those
/// because they may interact with other custom impls.
///
/// **Crate-path override**: add `#[entity(crate = "noxu_persist")]` to
/// the struct to direct generated code to `::noxu_persist::…` instead
/// of `::noxu::persist::…`.
#[proc_macro_derive(PrimaryKey, attributes(entity))]
pub fn derive_primary_key(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_primary_key(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand_primary_key(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let struct_ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) =
        input.generics.split_for_impl();

    let krate = parse_krate_from_entity_attr(&input.attrs)?;

    let data = match &input.data {
        Data::Struct(s) => s,
        _ => {
            return Err(syn::Error::new(
                input.span(),
                "`#[derive(PrimaryKey)]` can only be applied to structs",
            ));
        }
    };

    let (to_bytes_body, from_bytes_body) = match &data.fields {
        Fields::Unnamed(unnamed) if unnamed.unnamed.len() == 1 => {
            // Newtype: delegate.
            let inner_ty = &unnamed.unnamed[0].ty;
            (
                quote! {
                    <#inner_ty as #krate::PrimaryKey>::to_bytes(&self.0)
                },
                quote! {
                    Ok(Self(<#inner_ty as #krate::PrimaryKey>::from_bytes(bytes)?))
                },
            )
        }
        Fields::Unnamed(unnamed) => {
            // Tuple struct with N>1 fields: composite by index.
            let n = unnamed.unnamed.len();
            let idxs = (0..n).map(syn::Index::from).collect::<Vec<_>>();
            let tys = unnamed.unnamed.iter().map(|f| &f.ty).collect::<Vec<_>>();
            let to_bytes = composite_to_bytes_tuple(&idxs, &tys, &krate);
            let from_bytes = composite_from_bytes_tuple(&idxs, &tys, &krate);
            (to_bytes, from_bytes)
        }
        Fields::Named(named) => {
            let idents = named
                .named
                .iter()
                .map(|f| f.ident.as_ref().unwrap().clone())
                .collect::<Vec<_>>();
            let tys = named.named.iter().map(|f| &f.ty).collect::<Vec<_>>();
            let to_bytes = composite_to_bytes_named(&idents, &tys, &krate);
            let from_bytes = composite_from_bytes_named(&idents, &tys, &krate);
            (to_bytes, from_bytes)
        }
        Fields::Unit => {
            return Err(syn::Error::new(
                input.span(),
                "`#[derive(PrimaryKey)]` cannot be applied to unit structs \
                 (no key bytes to encode)",
            ));
        }
    };

    Ok(quote! {
        impl #impl_generics #krate::PrimaryKey for #struct_ident #ty_generics #where_clause {
            fn to_bytes(&self) -> ::std::vec::Vec<u8> {
                #to_bytes_body
            }

            fn from_bytes(bytes: &[u8]) -> #krate::Result<Self> {
                #from_bytes_body
            }
        }
    })
}

fn composite_to_bytes_tuple(
    idxs: &[syn::Index],
    tys: &[&Type],
    krate: &Path,
) -> TokenStream2 {
    let pieces = idxs.iter().zip(tys.iter()).map(|(i, ty)| {
        quote! {
            let part = <#ty as #krate::PrimaryKey>::to_bytes(&self.#i);
            buf.extend_from_slice(&(part.len() as u32).to_be_bytes());
            buf.extend_from_slice(&part);
        }
    });
    quote! {
        let mut buf: ::std::vec::Vec<u8> = ::std::vec::Vec::new();
        #(#pieces)*
        buf
    }
}

fn composite_from_bytes_tuple(
    idxs: &[syn::Index],
    tys: &[&Type],
    krate: &Path,
) -> TokenStream2 {
    let n = idxs.len();
    let var_decls = (0..n).map(|i| {
        let var = format_ident!("_part_{}", i);
        let ty = tys[i];
        quote! {
            let len = read_u32(bytes, &mut pos)? as usize;
            check_remaining(bytes, pos, len)?;
            let #var = <#ty as #krate::PrimaryKey>::from_bytes(&bytes[pos..pos + len])?;
            pos += len;
        }
    });
    let var_uses = (0..n).map(|i| format_ident!("_part_{}", i));
    quote! {
        fn read_u32(bytes: &[u8], pos: &mut usize) -> #krate::Result<u32> {
            if bytes.len() < *pos + 4 {
                return Err(#krate::PersistError::SerializationError(
                    "short read decoding composite key length prefix".into(),
                ));
            }
            let v = u32::from_be_bytes([
                bytes[*pos], bytes[*pos + 1], bytes[*pos + 2], bytes[*pos + 3],
            ]);
            *pos += 4;
            Ok(v)
        }
        fn check_remaining(bytes: &[u8], pos: usize, need: usize) -> #krate::Result<()> {
            if bytes.len() < pos + need {
                return Err(#krate::PersistError::SerializationError(
                    "short read decoding composite key field".into(),
                ));
            }
            Ok(())
        }
        let mut pos: usize = 0;
        #(#var_decls)*
        Ok(Self(#(#var_uses,)*))
    }
}

fn composite_to_bytes_named(
    idents: &[syn::Ident],
    tys: &[&Type],
    krate: &Path,
) -> TokenStream2 {
    let pieces = idents.iter().zip(tys.iter()).map(|(name, ty)| {
        quote! {
            let part = <#ty as #krate::PrimaryKey>::to_bytes(&self.#name);
            buf.extend_from_slice(&(part.len() as u32).to_be_bytes());
            buf.extend_from_slice(&part);
        }
    });
    quote! {
        let mut buf: ::std::vec::Vec<u8> = ::std::vec::Vec::new();
        #(#pieces)*
        buf
    }
}

fn composite_from_bytes_named(
    idents: &[syn::Ident],
    tys: &[&Type],
    krate: &Path,
) -> TokenStream2 {
    let var_decls = idents.iter().zip(tys.iter()).map(|(name, ty)| {
        quote! {
            let len = read_u32(bytes, &mut pos)? as usize;
            check_remaining(bytes, pos, len)?;
            let #name = <#ty as #krate::PrimaryKey>::from_bytes(&bytes[pos..pos + len])?;
            pos += len;
        }
    });
    quote! {
        fn read_u32(bytes: &[u8], pos: &mut usize) -> #krate::Result<u32> {
            if bytes.len() < *pos + 4 {
                return Err(#krate::PersistError::SerializationError(
                    "short read decoding composite key length prefix".into(),
                ));
            }
            let v = u32::from_be_bytes([
                bytes[*pos], bytes[*pos + 1], bytes[*pos + 2], bytes[*pos + 3],
            ]);
            *pos += 4;
            Ok(v)
        }
        fn check_remaining(bytes: &[u8], pos: usize, need: usize) -> #krate::Result<()> {
            if bytes.len() < pos + need {
                return Err(#krate::PersistError::SerializationError(
                    "short read decoding composite key field".into(),
                ));
            }
            Ok(())
        }
        let mut pos: usize = 0;
        #(#var_decls)*
        Ok(Self { #(#idents,)* })
    }
}

// ============================================================================
// #[derive(SecondaryKey)]
// ============================================================================

/// Derive secondary-index helpers for fields annotated with
/// `#[secondary_key(...)]`.
///
/// For every annotated field, emits an inherent method
/// `Foo::open_<name>_index(primary)` that registers a
/// `noxu_persist::SecondaryIndex` against the supplied primary index.
///
/// Example:
///
/// ```ignore
/// #[derive(Clone, Entity, SecondaryKey)]
/// struct User {
///     #[primary_key] id: u64,
///     #[secondary_key(name = "by_email", relate = OneToOne)] email: String,
///     #[secondary_key(name = "by_dept",  relate = ManyToOne)] dept: Option<u64>,
/// }
/// ```
///
/// Generates `User::open_by_email_index(&mut PrimaryIndex<u64, User>) ->
/// SecondaryIndex<String, u64, User>` and `User::open_by_dept_index(...)
/// -> SecondaryIndex<u64, u64, User>` (the `Option<u64>` is unwrapped to
/// the inner type for `SK`).
///
/// Also emits `pub const SECONDARY_INDEXES: &'static [SecondarySpec]` on
/// the struct, suitable for runtime introspection.
///
/// **Crate-path override**: add `#[entity(crate = "noxu_persist")]` to
/// the struct to direct generated code to `::noxu_persist::…` instead
/// of `::noxu::persist::…`.
#[proc_macro_derive(
    SecondaryKey,
    attributes(secondary_key, primary_key, entity)
)]
pub fn derive_secondary_key(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_secondary_key(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand_secondary_key(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let struct_ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) =
        input.generics.split_for_impl();
    let fields = struct_fields(input)?;

    let krate = parse_krate_from_entity_attr(&input.attrs)?;

    // Locate the primary-key field so we can type the helper signatures.
    let pk_field = find_primary_key_field(fields)?;
    let pk_ty = &pk_field.ty;

    // Collect every #[secondary_key(...)] field.
    let mut specs = Vec::new();
    for field in fields {
        for attr in &field.attrs {
            if !attr.path().is_ident("secondary_key") {
                continue;
            }
            specs.push(parse_secondary_key_attr(attr, field)?);
        }
    }

    if specs.is_empty() {
        return Err(syn::Error::new(
            input.span(),
            "`#[derive(SecondaryKey)]` requires at least one field with \
             `#[secondary_key(name = \"...\", relate = ...)]`",
        ));
    }

    let mut helpers = Vec::with_capacity(specs.len());
    let mut spec_consts = Vec::with_capacity(specs.len());

    for s in &specs {
        let field_ident = &s.field_ident;
        let helper_name =
            format_ident!("open_{}_index", sanitise_ident(&s.name));
        let sk_ty = &s.sk_inner_ty;
        let extractor = if s.is_optional {
            quote! { |__e: &Self| __e.#field_ident.clone() }
        } else {
            quote! { |__e: &Self| ::std::option::Option::Some(__e.#field_ident.clone()) }
        };

        let name_lit = &s.name;
        let relate_tok = relate_to_tokens(&s.relate, &krate);
        let related_tok = match &s.related_entity {
            Some(r) => quote! { ::std::option::Option::Some(#r) },
            None => quote! { ::std::option::Option::None },
        };
        let action_tok = delete_action_to_tokens(&s.on_delete, &krate);

        helpers.push(quote! {
            #[doc = concat!(
                "Opens the `",
                #name_lit,
                "` secondary index against the supplied primary index. \
                 Auto-generated by `#[derive(SecondaryKey)]`.",
            )]
            pub fn #helper_name<'__pidx>(
                primary: &mut #krate::PrimaryIndex<'__pidx, #pk_ty, Self>,
            ) -> #krate::SecondaryIndex<#sk_ty, #pk_ty, Self> {
                primary.open_secondary_index(#extractor)
            }
        });

        spec_consts.push(quote! {
            #krate::SecondarySpec {
                name: #name_lit,
                relate: #relate_tok,
                related_entity: #related_tok,
                on_related_entity_delete: #action_tok,
            }
        });
    }

    let n = specs.len();

    Ok(quote! {
        impl #impl_generics #struct_ident #ty_generics #where_clause {
            /// Compile-time metadata for every `#[secondary_key(...)]` field
            /// declared on this entity.  Auto-generated by
            /// `#[derive(SecondaryKey)]`.
            pub const SECONDARY_INDEXES: &'static [#krate::SecondarySpec; #n] = &[
                #(#spec_consts),*
            ];

            #(#helpers)*
        }
    })
}

// ============================================================================
// Shared helpers
// ============================================================================

fn struct_fields(
    input: &DeriveInput,
) -> syn::Result<&syn::punctuated::Punctuated<Field, Token![,]>> {
    match &input.data {
        Data::Struct(DataStruct { fields: Fields::Named(named), .. }) => {
            Ok(&named.named)
        }
        Data::Struct(_) => Err(syn::Error::new(
            input.span(),
            "this derive requires a struct with named fields",
        )),
        _ => Err(syn::Error::new(
            input.span(),
            "this derive can only be applied to structs",
        )),
    }
}

fn find_primary_key_field(
    fields: &syn::punctuated::Punctuated<Field, Token![,]>,
) -> syn::Result<&Field> {
    let mut found: Option<&Field> = None;
    for field in fields {
        if field.attrs.iter().any(|a| a.path().is_ident("primary_key")) {
            if let Some(prev) = found {
                let mut e = syn::Error::new(
                    field.span(),
                    "multiple `#[primary_key]` fields are not supported; \
                     use `#[derive(PrimaryKey)]` on a composite key struct \
                     and reference it from a single primary-key field",
                );
                e.combine(syn::Error::new(
                    prev.span(),
                    "first `#[primary_key]` field is here",
                ));
                return Err(e);
            }
            found = Some(field);
        }
    }
    found.ok_or_else(|| {
        syn::Error::new(
            fields.span(),
            "missing `#[primary_key]` field — exactly one field must be \
             annotated `#[primary_key]` on a `#[derive(Entity)]` struct",
        )
    })
}

#[derive(Clone)]
struct ParsedSecondary {
    field_ident: syn::Ident,
    /// The `SK` type used by the generated `SecondaryIndex<SK, PK, E>` —
    /// equals the field type if the field is non-`Option`, or the inner
    /// type if the field is `Option<T>`.
    sk_inner_ty: Type,
    is_optional: bool,
    name: String,
    relate: String,
    related_entity: Option<String>,
    on_delete: String,
}

fn parse_secondary_key_attr(
    attr: &Attribute,
    field: &Field,
) -> syn::Result<ParsedSecondary> {
    let field_ident = field.ident.clone().ok_or_else(|| {
        syn::Error::new(
            field.span(),
            "`#[secondary_key(...)]` requires a named field",
        )
    })?;

    let (is_optional, sk_inner_ty) = unwrap_option_type(&field.ty);

    let mut name: Option<String> = None;
    let mut relate: Option<String> = None;
    let mut related_entity: Option<String> = None;
    let mut on_delete: Option<String> = None;

    let meta = match &attr.meta {
        Meta::List(list) => list,
        _ => {
            return Err(syn::Error::new(
                attr.span(),
                "expected `#[secondary_key(name = \"...\", relate = ..., \
                 [related_entity = \"...\"], [on_related_entity_delete = ...])]`",
            ));
        }
    };

    meta.parse_nested_meta(|m| {
        if m.path.is_ident("name") {
            let v: syn::LitStr = m.value()?.parse()?;
            name = Some(v.value());
            Ok(())
        } else if m.path.is_ident("relate") {
            let v: Expr = m.value()?.parse()?;
            relate = Some(expr_to_ident_string(&v)?);
            Ok(())
        } else if m.path.is_ident("related_entity") {
            let v: syn::LitStr = m.value()?.parse()?;
            related_entity = Some(v.value());
            Ok(())
        } else if m.path.is_ident("on_related_entity_delete") {
            let v: Expr = m.value()?.parse()?;
            on_delete = Some(expr_to_ident_string(&v)?);
            Ok(())
        } else {
            Err(m.error(
                "unrecognised key in `#[secondary_key(...)]`; allowed \
                 keys: name, relate, related_entity, on_related_entity_delete",
            ))
        }
    })?;

    let name = name.ok_or_else(|| {
        syn::Error::new(
            attr.span(),
            "`#[secondary_key(...)]` requires `name = \"...\"`",
        )
    })?;
    let relate = relate.ok_or_else(|| {
        syn::Error::new(
            attr.span(),
            "`#[secondary_key(...)]` requires `relate = OneToOne | \
             ManyToOne | OneToMany | ManyToMany`",
        )
    })?;
    if !matches!(
        relate.as_str(),
        "OneToOne" | "ManyToOne" | "OneToMany" | "ManyToMany"
    ) {
        return Err(syn::Error::new(
            attr.span(),
            format!(
                "invalid `relate = {relate}`; allowed values: OneToOne, \
                 ManyToOne, OneToMany, ManyToMany"
            ),
        ));
    }

    let on_delete = on_delete.unwrap_or_else(|| "Abort".to_string());
    if !matches!(
        on_delete.as_str(),
        "Abort" | "Cascade" | "Nullify" | "ABORT" | "CASCADE" | "NULLIFY"
    ) {
        return Err(syn::Error::new(
            attr.span(),
            format!(
                "invalid `on_related_entity_delete = {on_delete}`; allowed \
                 values: Abort, Cascade, Nullify (BDB-JE-style ABORT, \
                 CASCADE, NULLIFY are also accepted)"
            ),
        ));
    }
    if name.is_empty() {
        return Err(syn::Error::new(
            attr.span(),
            "`#[secondary_key(name = \"\")]` is empty; index names are \
             used as part of helper-method identifiers and must be non-empty",
        ));
    }

    Ok(ParsedSecondary {
        field_ident,
        sk_inner_ty,
        is_optional,
        name,
        relate,
        related_entity,
        on_delete: normalise_action(&on_delete),
    })
}

fn normalise_action(s: &str) -> String {
    match s {
        "ABORT" => "Abort".to_string(),
        "CASCADE" => "Cascade".to_string(),
        "NULLIFY" => "Nullify".to_string(),
        _ => s.to_string(),
    }
}

fn expr_to_ident_string(expr: &Expr) -> syn::Result<String> {
    match expr {
        Expr::Path(p) => Ok(p
            .path
            .segments
            .last()
            .map(|s| s.ident.to_string())
            .unwrap_or_default()),
        Expr::Lit(ExprLit { lit: Lit::Str(s), .. }) => Ok(s.value()),
        _ => Err(syn::Error::new(
            expr.span(),
            "expected an identifier (e.g. `OneToOne`) or a string literal",
        )),
    }
}

fn relate_to_tokens(s: &str, krate: &Path) -> TokenStream2 {
    let id = format_ident!("{}", s);
    quote! { #krate::Relate::#id }
}

fn delete_action_to_tokens(s: &str, krate: &Path) -> TokenStream2 {
    let id = format_ident!("{}", s);
    quote! { #krate::DeleteAction::#id }
}

/// Returns `(is_option, inner_ty_or_self)` for the supplied type.
///
/// Recognises the following spellings:
/// - `Option<T>`
/// - `std::option::Option<T>`
/// - `core::option::Option<T>`
fn unwrap_option_type(ty: &Type) -> (bool, Type) {
    if let Type::Path(TypePath { qself: None, path }) = ty {
        let segs: Vec<String> =
            path.segments.iter().map(|s| s.ident.to_string()).collect();
        let is_option = matches!(
            segs.as_slice(),
            [a] if a == "Option"
        ) || matches!(
            segs.as_slice(),
            [a, b, c] if (a == "std" || a == "core") && b == "option" && c == "Option"
        );
        if is_option
            && let Some(last) = path.segments.last()
            && let PathArguments::AngleBracketed(args) = &last.arguments
            && let Some(GenericArgument::Type(inner)) = args.args.first()
        {
            return (true, inner.clone());
        }
    }
    (false, ty.clone())
}

/// Normalise an arbitrary index name like `"by_email"` or `"by-email"` into
/// a Rust identifier suffix like `by_email`.  The macro forbids non-empty
/// names, so the result is always a non-empty ident.
fn sanitise_ident(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut first = true;
    for ch in name.chars() {
        if first {
            if ch.is_ascii_digit() {
                out.push('_');
            }
            first = false;
        }
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        out.push('_');
    }
    out
}
