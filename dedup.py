import mimetypes
import pyexiv2
import sqlite3
import hashlib
import os
import sys

#"CREATE TABLE (string path, length, md5, mimetype, encoding, mtime)"
#"CREATE TABLE (string path, camera_make, camera_model, date_taken)"
#"CREATE TABLE (string path)"

DB="fileinfo.db"

def compute_file_info(full_path):
  print full_path
  try:
    s = os.stat(full_path)
  except:
    return None
  print s.st_size, s.st_mtime, s.st_ctime
  
  mimetype, encoding = mimetypes.guess_type(full_path)
  print mimetype, encoding
  
  try:
    f = open(full_path, "rb")
  except:
    return None
  md5, sha = hashlib.md5(), hashlib.sha512()
  
  block_size = 0
  if md5.block_size % sha.block_size == 0:
    block_size = md5.block_size
  elif sha.block_size % md5.block_size == 0:
    block_size = sha.block_size
  else:
    block_size = md5.block_size * sha.block_size
  
  b = f.read(block_size)
  while b != "":
    md5.update(b)
    sha.update(b)
    b = f.read(block_size)
  f.close()
  print md5.hexdigest(), sha.hexdigest()
  
  return [s.st_size, md5.hexdigest(), mimetype, encoding, s.st_mtime]
  
def compute_pic_info(full_path):
  metadata = pyexiv2.metadata.ImageMetadata(full_path)
  metadata.read()
  mimetype = metadata.mime_type
  
  if "Exif.Image.Model" not in metadata.exif_keys or "Exif.Photo.DateTimeOriginal" not in metadata.exif_keys:
    return None
  return [metadata["Exif.Image.Make"].value,
          metadata["Exif.Image.Model"].value,
          metadata["Exif.Photo.DateTimeOriginal"].value]

def crawl_dirs(db, path):
  for fn in os.listdir(path):
    full_path = unicode(os.path.join(path, fn))
    if os.path.isdir(full_path):
      c = db.cursor()
      c.execute("SELECT path FROM crawled WHERE path = ?", (full_path, ))
      if len(c.fetchall()) == 0:
        crawl_dirs(db, full_path)
      else:
        print "Skipping:", full_path 
    else:
      file_info = compute_file_info(full_path)
      if file_info:
        db.execute("insert into file_info values (?, ?, ?, ?, ?, ?)", [full_path] + file_info)
        if file_info[2] in ["image/jpeg"]:
          pic_info = compute_pic_info(full_path)
          if pic_info:
            db.execute("insert into pic_info values (?, ?, ?, ?)", [full_path] + pic_info)
  db.execute("insert into crawled values (?)", (path, ))
  db.commit()

db = sqlite3.connect('/tmp/test.db')

c = db.cursor()
c.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
if len(c.fetchall()) <= 0:
  print "creating tables"
  db.execute("CREATE TABLE file_info (path TEXT, " +
                                     "length INTEGER, "+
                                     "md5 BLOB, " +
                                     "mimetype TEXT, " +
                                     "encoding TEXT, " +
                                     "mtime INTEGER)")
  db.execute("CREATE TABLE pic_info(path TEXT, " +
                                   "camera_make TEXT, " +
                                   "camera_model TEXT, " +
                                   "date_taken INTEGER)")
  db.execute("CREATE TABLE crawled (path TEXT)")
crawl_dirs(db, sys.argv[1])
db.close()