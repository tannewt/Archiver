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
  try:
    s = os.stat(full_path)
  except:
    return None
  print s.st_size, s.st_mtime, s.st_ctime
  
  mimetype, encoding = mimetypes.guess_type(full_path)
  print mimetype, encoding
 
  if s.st_size == 0:
    return [s.st_size, None, None, mimetype, encoding, s.st_mtime]
 
  try:
    f = open(full_path, "rb")
  except IOError as e:
    return e
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
  
  return [s.st_size, md5.hexdigest(), sha.hexdigest(), mimetype, encoding, s.st_mtime]
  
def compute_pic_info(full_path):
  metadata = pyexiv2.metadata.ImageMetadata(full_path)
  try:
    metadata.read()
  except IOError as e:
    print e
    return None
  mimetype = metadata.mime_type
  
  if "Exif.Image.Model" not in metadata.exif_keys or "Exif.Photo.DateTimeOriginal" not in metadata.exif_keys:
    return None
  return [metadata["Exif.Image.Make"].value,
          metadata["Exif.Image.Model"].value,
          metadata["Exif.Photo.DateTimeOriginal"].value]

def decode_string(s, encodings=('ascii', 'utf-8', 'latin1')):
  for encoding in encodings:
    try:
      return s.decode(encoding)
    except UnicodeDecodeError:
      pass
  return s.decode(ascii, 'replace')

def crawl_dirs(db, path, count=0):
  try:
    filenames = os.listdir(path)
  except OSError as e:
    db.execute("insert into unreadable values (?, ?)", (path, str(e)))
    print e
    return count
  for fn in filenames:
    try:
      full_path = path.encode("utf-8") + u"/" + decode_string(fn)
    except UnicodeError as e:
      print e
      db.execute(u"insert into unreadable values (?, ?)", (unicode(path) + u"/" + unicode(fn), unicode(e)))
      continue
    c = db.cursor()
    if os.path.islink(full_path):
      continue
    if os.path.isdir(full_path):
      c.execute("SELECT path FROM crawled WHERE path = ?", (full_path, ))
      if len(c.fetchall()) == 0:
        count = crawl_dirs(db, full_path, count)
      else:
        print "Skipping:", full_path 
    else:
      count += 1
      print "#" + str(count), full_path
      c.execute("SELECT path FROM file_info WHERE path = ?", (full_path, ))
      if len(c.fetchall()) > 0:
        print "Skipping:", full_path
        continue
      file_info = compute_file_info(full_path)
      if type(file_info) is IOError:
        db.execute("insert into unreadable values (?, ?)", (full_path, str(file_info)))
      elif file_info:
        db.execute("insert into file_info values (?, ?, ?, ?, ?, ?, ?)", [full_path] + file_info)
        if file_info[4] in ["image/jpeg"]:
          pic_info = compute_pic_info(full_path)
          if pic_info:
	    print " ".join([str(x) for x in pic_info])
            db.execute("insert into pic_info values (?, ?, ?, ?)", [full_path] + pic_info)
      print
    c.close()
  db.execute("insert into crawled values (?)", (path, ))
  db.commit()
  return count

db = sqlite3.connect(sys.argv[2])

c = db.cursor()
c.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
if len(c.fetchall()) <= 0:
  print "creating tables"
  db.execute("CREATE TABLE file_info (path TEXT, " +
                                     "length INTEGER, "+
                                     "md5 BLOB, " +
				     "sha BLOB, " +
                                     "mimetype TEXT, " +
                                     "encoding TEXT, " +
                                     "mtime INTEGER)")
  db.execute("CREATE INDEX file_path_index ON file_info (path)");
  db.execute("CREATE TABLE pic_info(path TEXT, " +
                                   "camera_make TEXT, " +
                                   "camera_model TEXT, " +
                                   "date_taken INTEGER)")
  db.execute("CREATE TABLE crawled (path TEXT)")
  db.execute("CREATE TABLE unreadable (path TEXT, message TEXT)")
print crawl_dirs(db, sys.argv[1]), "total files crawled"
db.close()
