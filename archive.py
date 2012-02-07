#!/usr/bin/python2.7

import subprocess
import sys

from boto.s3.connection import S3Connection
from boto.s3.key import Key

class Archiver:
  S3_KEY_ID = 0
  S3_SECRET_KEY = 0
  NOOP = True
  ARCHIVE_BASE = "/"
  
  def ComputeMetadata(self, path):
    """Compute metadata about the given file."""
    # For deduplication:
    # md5
    # sha
    # length
    
    # For convenience:
    # enc-content-type
    pass

  def EncryptAndUploadToS3(bucket, filename, metadata):
    """Encrypts the given file and uploads it to S3.

      This operation is idempotent."""

    tmp_fp = os.tmpfile()
    subprocess.call("cat", filename, stdout=tmp_fp)
    metadata['enc-method'] = 0
    # Check for an existing S3 path xattr on the file.
    # Check if the uploaded hash matches this new one.
    if NOOP:
      print bucket, key, metadata, filename
    else:
      key = Key(bucket)
      key.update_metadata(metadata)
      key.key(filename)
      key.set_contents_from_file(tmp_fp, cb=, num_cb=100)

  def LintFilename():

  def LoadConfiguration():
    
  def Connect(self):
    self.boto_connection = S3Connection(S3_KEY_ID, S3_SECRET_KEY)
    
  def Archive(self, path):
    metadata = ComputeMetadata(path)
    
  def Run(self, path=INITIAL_PATH):
    for fn in os.listdir(path):
      fn = os.path.join(path, fn)
      if os.path.isdir(fn):
        self.Run(fn)
      else:
        self.Archive(fn)
    
if __name__=="__main__":
  a = Archiver()
  a.Run()