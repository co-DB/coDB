# Serialization

## Zero-Copy Serialization and Deserialization

TODO: please leave a short comment about this - how and when to use it once its researched @Polyphemus980

## Endianness

All manual binary serialization and deserialization performed inside the engine crate MUST use little-endian byte order. 
In case of zero-copy serialization and deserialization (unsafe casting) we rely on on-disk format.