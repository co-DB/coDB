use crate::catalog::CatalogError;
use crate::consts::METADATA_FILE_NAME;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::{fs, time};

/// Helper struct that provides utility functions for managing metadata files
/// and their temporary counterparts.
pub(crate) struct MetadataFileHelper {}

impl MetadataFileHelper {
    /// Removes all temporary files from the file system.
    /// Can fail if io error occurs.
    fn remove_tmp_files(tmp_files: &mut Vec<PathBuf>) -> Result<(), CatalogError> {
        for tmp_path in tmp_files {
            fs::remove_file(tmp_path)?;
        }
        Ok(())
    }

    /// Returns the latest version of metadata saved on disk.
    ///
    /// Most of the time it will just return content of the main metadata file,
    /// but in cases when there was a problem during a write operation and new content
    /// was only saved to a temporary file, it will return the content from the newest temporary file.
    ///
    /// The function uses a provided `load_fn` to deserialize the metadata from the file.
    ///
    /// Can fail if io error occurs or file was not properly formatted.
    pub(crate) fn latest_catalog_json<T>(
        main_dir_path: impl AsRef<Path>,
        database_name: &str,
        load_fn: impl Fn(&Path) -> Result<T, CatalogError>,
    ) -> Result<T, CatalogError> {
        let db_dir = main_dir_path.as_ref().join(database_name);
        let main_file = db_dir.join(METADATA_FILE_NAME);
        let tmp_file_prefix = format!("{}.tmp-", METADATA_FILE_NAME);
        let mut tmp_files = Self::list_catalog_tmp_files(&db_dir, &tmp_file_prefix)?;

        let catalog_json_tmp =
            Self::find_latest_valid_tmp_catalog::<T>(&main_file, &mut tmp_files, &load_fn)?;

        Self::remove_tmp_files(&mut tmp_files)?;

        let catalog_json = catalog_json_tmp.unwrap_or(load_fn(&main_file)?);

        Ok(catalog_json)
    }

    /// Returns a list of all temporary files in the directory at `directory_path` with the given `file_prefix`,
    /// sorted by their epoch timestamp (the latest is the last).
    ///
    /// Temporary files are expected to follow the naming pattern: `{file_prefix}{epoch}`,
    /// where `{epoch}` is a timestamp in milliseconds since UNIX_EPOCH.
    ///
    /// Can fail if io error occurs.
    pub(crate) fn list_catalog_tmp_files(
        directory_path: impl AsRef<Path>,
        file_prefix: &str,
    ) -> Result<Vec<PathBuf>, CatalogError> {
        let mut tmp_files = fs::read_dir(directory_path)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                let file_name = path.file_name()?.to_string_lossy();
                if file_name.starts_with(file_prefix)
                    && let Ok(epoch) = file_name[file_prefix.len()..].parse::<u128>()
                {
                    return Some((epoch, path));
                }
                None
            })
            .collect::<Vec<(u128, PathBuf)>>();

        tmp_files.sort_by_key(|(epoch, _)| *epoch);

        Ok(tmp_files.into_iter().map(|(_, p)| p).collect())
    }

    /// Iterates over elements in `tmp_files` (sorted by epoch, latest last) and tries to find one that has:
    ///
    /// - modification time > main file modification time
    /// - valid metadata structure as its content (validated by `load_fn`)
    ///
    /// If successfully found a tmp file that matches these requirements, returns `Some(T)` - the metadata loaded from that tmp file.
    /// The valid tmp file is then renamed to replace the main file.
    ///
    /// If no such tmp file was found, returns `None`, meaning that metadata should be loaded from the main file.
    ///
    /// Each element consumed from `tmp_files` is guaranteed to be either:
    /// - Renamed to replace the main file (if valid and newer)
    /// - Removed from the file system (if invalid or older than main file)
    ///
    /// Can fail if io error occurs.
    pub(crate) fn find_latest_valid_tmp_catalog<T>(
        main_file: impl AsRef<Path>,
        tmp_files: &mut Vec<PathBuf>,
        load_fn: &impl Fn(&Path) -> Result<T, CatalogError>,
    ) -> Result<Option<T>, CatalogError> {
        let main_mtime = Self::file_last_modified_time(&main_file);

        let mut catalog_json = None;

        while let Some(tmp_path) = tmp_files.pop() {
            let tmp_mtime = Self::file_last_modified_time(&tmp_path);
            let tmp_is_latest = tmp_mtime > main_mtime;
            match tmp_is_latest {
                true => {
                    let tmp_catalog_json = load_fn(&tmp_path);

                    // Successfully deserialized valid metadata structure from the file.
                    // This is assumed to be the most recent version that should be used.
                    if let Ok(cj) = tmp_catalog_json {
                        fs::rename(&tmp_path, &main_file)?;
                        catalog_json = Some(cj);
                        break;
                    }
                    // Failed to read valid metadata from file - remove it as it's corrupted.
                    fs::remove_file(&tmp_path)?;
                }
                false => {
                    // If main file has the latest modification time then we no longer need tmp file and can remove it.
                    fs::remove_file(&tmp_path)?;
                    break;
                }
            };
        }

        Ok(catalog_json)
    }

    /// Safely writes metadata to disk using a temporary file for atomic writes.
    ///
    /// This ensures atomic writes: the content is first written to a temporary file,
    /// synced to disk, and only then renamed to replace the main file.
    /// If any step fails, the main file remains unchanged.
    ///
    /// The process:
    /// 1. Serialize data using the provided `serialize_fn`
    /// 2. Create a temporary file with name `{file_path}.tmp-{epoch}`
    /// 3. Write serialized content to temporary file
    /// 4. Sync data to disk (fsync)
    /// 5. Atomically rename temporary file to replace the main file
    ///
    /// The `serialize_fn` should convert the data into a string format (e.g., JSON).
    ///
    /// Can fail if io error occurs or serialization fails.
    pub(crate) fn sync_to_disk<T>(
        file_path: impl AsRef<Path>,
        data: &T,
        serialize_fn: impl Fn(&T) -> Result<String, CatalogError>,
    ) -> Result<(), CatalogError> {
        let content = serialize_fn(data)?;

        // We can unwrap here, because we know `UNIX_EPOCH` was before `SystemTime::now`
        let epoch = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let tmp_path = file_path.as_ref().with_extension(format!("tmp-{epoch}"));

        // We firstly store content in temporary file, only when all content is successfully saved to file
        // we swap it with previous file content.
        let mut tmp_file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;

        tmp_file.write_all(content.as_bytes())?;
        tmp_file.sync_data()?;

        fs::rename(&tmp_path, file_path.as_ref())?;

        Ok(())
    }

    /// Returns time of last file modification.
    /// For convenience in case of error just returns UNIX_EPOCH.
    fn file_last_modified_time(path: impl AsRef<Path>) -> time::SystemTime {
        let meta = fs::metadata(path);
        meta.and_then(|m| m.modified()).unwrap_or(time::UNIX_EPOCH)
    }
}
