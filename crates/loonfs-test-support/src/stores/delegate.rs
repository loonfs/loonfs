//! Forwarding macros for small object-store test doubles.

/// Emits forwarding methods for an `ObjectStore` implementation.
#[macro_export]
macro_rules! delegate_object_store {
    ($receiver:ident => $inner:expr) => {
        $crate::delegate_object_store!($receiver => $inner;
            head,
            create_multipart_upload,
            complete_multipart_upload,
            abort_multipart_upload,
            get_with_metadata,
            get,
            put,
            delete,
            list_prefix_from_stream,
        );
    };
    ($receiver:ident => $inner:expr; except get) => {
        $crate::delegate_object_store!($receiver => $inner;
            head,
            create_multipart_upload,
            complete_multipart_upload,
            abort_multipart_upload,
            get_with_metadata,
            put,
            delete,
            list_prefix_from_stream,
        );
    };
    ($receiver:ident => $inner:expr; except put) => {
        $crate::delegate_object_store!($receiver => $inner;
            head,
            create_multipart_upload,
            complete_multipart_upload,
            abort_multipart_upload,
            get_with_metadata,
            get,
            delete,
            list_prefix_from_stream,
        );
    };
    ($receiver:ident => $inner:expr; $($method:ident),+ $(,)?) => {
        $(
            $crate::__delegate_object_store_method!($method, $receiver, $inner);
        )+
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __delegate_object_store_method {
    (head, $receiver:ident, $inner:expr) => {
        fn head<'store, 'key, 'future>(
            &'store $receiver,
            key: &'key str,
        ) -> ::core::pin::Pin<::std::boxed::Box<
            dyn ::core::future::Future<
                    Output = Result<
                        Option<::loonfs_objectstore::ObjectMetadata>,
                        ::loonfs_objectstore::ObjectStoreError,
                    >,
                > + Send
                + 'future,
        >>
        where
            'store: 'future,
            'key: 'future,
            Self: 'future,
        {
            ::std::boxed::Box::pin(async move { $inner.head(key).await })
        }
    };
    (head_stored_checksum, $receiver:ident, $inner:expr) => {
        fn head_stored_checksum<'store, 'key, 'future>(
            &'store $receiver,
            key: &'key str,
        ) -> ::core::pin::Pin<::std::boxed::Box<
            dyn ::core::future::Future<
                    Output = Result<
                        Option<::loonfs_objectstore::StoredObjectChecksum>,
                        ::loonfs_objectstore::ObjectStoreError,
                    >,
                > + Send
                + 'future,
        >>
        where
            'store: 'future,
            'key: 'future,
            Self: 'future,
        {
            ::std::boxed::Box::pin(async move { $inner.head_stored_checksum(key).await })
        }
    };
    (create_multipart_upload, $receiver:ident, $inner:expr) => {
        fn create_multipart_upload<'store, 'key, 'future>(
            &'store $receiver,
            key: &'key str,
        ) -> ::core::pin::Pin<::std::boxed::Box<
            dyn ::core::future::Future<
                    Output = Result<String, ::loonfs_objectstore::ObjectStoreError>,
                > + Send
                + 'future,
        >>
        where
            'store: 'future,
            'key: 'future,
            Self: 'future,
        {
            ::std::boxed::Box::pin(async move { $inner.create_multipart_upload(key).await })
        }
    };
    (complete_multipart_upload, $receiver:ident, $inner:expr) => {
        fn complete_multipart_upload<
            'store,
            'key,
            'upload,
            'parts,
            'checksum,
            'future,
        >(
            &'store $receiver,
            key: &'key str,
            provider_upload_id: &'upload str,
            parts: &'parts [::loonfs_objectstore::MultipartPart],
            checksum: &'checksum ::loonfs_api::Checksum,
        ) -> ::core::pin::Pin<::std::boxed::Box<
            dyn ::core::future::Future<
                    Output = Result<
                        ::loonfs_objectstore::MultipartCompletion,
                        ::loonfs_objectstore::ObjectStoreError,
                    >,
                > + Send
                + 'future,
        >>
        where
            'store: 'future,
            'key: 'future,
            'upload: 'future,
            'parts: 'future,
            'checksum: 'future,
            Self: 'future,
        {
            ::std::boxed::Box::pin(async move {
                $inner
                    .complete_multipart_upload(key, provider_upload_id, parts, checksum)
                    .await
            })
        }
    };
    (abort_multipart_upload, $receiver:ident, $inner:expr) => {
        fn abort_multipart_upload<'store, 'key, 'upload, 'future>(
            &'store $receiver,
            key: &'key str,
            provider_upload_id: &'upload str,
        ) -> ::core::pin::Pin<::std::boxed::Box<
            dyn ::core::future::Future<
                    Output = Result<(), ::loonfs_objectstore::ObjectStoreError>,
                > + Send
                + 'future,
        >>
        where
            'store: 'future,
            'key: 'future,
            'upload: 'future,
            Self: 'future,
        {
            ::std::boxed::Box::pin(async move {
                $inner
                    .abort_multipart_upload(key, provider_upload_id)
                    .await
            })
        }
    };
    (get_with_metadata, $receiver:ident, $inner:expr) => {
        fn get_with_metadata<'store, 'key, 'future>(
            &'store $receiver,
            key: &'key str,
        ) -> ::core::pin::Pin<::std::boxed::Box<
            dyn ::core::future::Future<
                    Output = Result<
                        Option<::loonfs_objectstore::ObjectBody>,
                        ::loonfs_objectstore::ObjectStoreError,
                    >,
                > + Send
                + 'future,
        >>
        where
            'store: 'future,
            'key: 'future,
            Self: 'future,
        {
            ::std::boxed::Box::pin(async move { $inner.get_with_metadata(key).await })
        }
    };
    (get, $receiver:ident, $inner:expr) => {
        fn get<'store, 'key, 'future>(
            &'store $receiver,
            key: &'key str,
            range: Option<::loonfs_objectstore::ByteRange>,
        ) -> ::core::pin::Pin<::std::boxed::Box<
            dyn ::core::future::Future<
                    Output = Result<
                        Option<::bytes::Bytes>,
                        ::loonfs_objectstore::ObjectStoreError,
                    >,
                > + Send
                + 'future,
        >>
        where
            'store: 'future,
            'key: 'future,
            Self: 'future,
        {
            ::std::boxed::Box::pin(async move { $inner.get(key, range).await })
        }
    };
    (put, $receiver:ident, $inner:expr) => {
        fn put<'store, 'key, 'future>(
            &'store $receiver,
            key: &'key str,
            bytes: ::bytes::Bytes,
            mode: ::loonfs_objectstore::PutMode,
        ) -> ::core::pin::Pin<::std::boxed::Box<
            dyn ::core::future::Future<
                    Output = Result<
                        ::loonfs_objectstore::ObjectMetadata,
                        ::loonfs_objectstore::ObjectStoreError,
                    >,
                > + Send
                + 'future,
        >>
        where
            'store: 'future,
            'key: 'future,
            Self: 'future,
        {
            ::std::boxed::Box::pin(async move { $inner.put(key, bytes, mode).await })
        }
    };
    (put_streamed, $receiver:ident, $inner:expr) => {
        fn put_streamed<'store, 'key, 'future>(
            &'store $receiver,
            key: &'key str,
            body: ::loonfs_objectstore::ByteStream,
            mode: ::loonfs_objectstore::PutMode,
        ) -> ::core::pin::Pin<::std::boxed::Box<
            dyn ::core::future::Future<
                    Output = Result<u64, ::loonfs_objectstore::ObjectStoreError>,
                > + Send
                + 'future,
        >>
        where
            'store: 'future,
            'key: 'future,
            Self: 'future,
        {
            ::std::boxed::Box::pin(async move { $inner.put_streamed(key, body, mode).await })
        }
    };
    (compare_and_swap, $receiver:ident, $inner:expr) => {
        fn compare_and_swap<'store, 'key, 'etag, 'future>(
            &'store $receiver,
            key: &'key str,
            expected_etag: &'etag str,
            bytes: ::bytes::Bytes,
        ) -> ::core::pin::Pin<::std::boxed::Box<
            dyn ::core::future::Future<
                    Output = Result<
                        ::loonfs_objectstore::ObjectMetadata,
                        ::loonfs_objectstore::ObjectStoreError,
                    >,
                > + Send
                + 'future,
        >>
        where
            'store: 'future,
            'key: 'future,
            'etag: 'future,
            Self: 'future,
        {
            ::std::boxed::Box::pin(async move {
                $inner.compare_and_swap(key, expected_etag, bytes).await
            })
        }
    };
    (delete, $receiver:ident, $inner:expr) => {
        fn delete<'store, 'key, 'future>(
            &'store $receiver,
            key: &'key str,
        ) -> ::core::pin::Pin<::std::boxed::Box<
            dyn ::core::future::Future<
                    Output = Result<(), ::loonfs_objectstore::ObjectStoreError>,
                > + Send
                + 'future,
        >>
        where
            'store: 'future,
            'key: 'future,
            Self: 'future,
        {
            ::std::boxed::Box::pin(async move { $inner.delete(key).await })
        }
    };
    (list_prefix_from_stream, $receiver:ident, $inner:expr) => {
        fn list_prefix_from_stream(
            &$receiver,
            prefix: &str,
            start_after: Option<&str>,
        ) -> ::futures::stream::BoxStream<
            'static,
            Result<String, ::loonfs_objectstore::ObjectStoreError>,
        > {
            $inner.list_prefix_from_stream(prefix, start_after)
        }
    };
}
