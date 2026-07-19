use std::sync::Arc;

use sukari::Bytes;

#[test]
fn bytes_shares_arc_payloads() {
    let shared: Arc<[u8]> = Arc::from(b"payload".as_slice());
    let bytes = Bytes::from(shared.clone());
    let cloned = bytes.clone();

    assert_eq!(bytes.as_slice(), b"payload");
    assert!(Arc::ptr_eq(bytes.as_arc(), &shared));
    assert!(Arc::ptr_eq(bytes.as_arc(), cloned.as_arc()));

    let exported: Arc<[u8]> = cloned.into();
    assert!(Arc::ptr_eq(&exported, &shared));
}

#[test]
fn bytes_into_vec_returns_owned_payload() {
    let bytes = Bytes::from(Arc::<[u8]>::from(b"payload".as_slice()));

    assert_eq!(bytes.into_vec(), b"payload");
}
