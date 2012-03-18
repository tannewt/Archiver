import pyexiv2
import sqlite3
import os
import sys

#"CREATE TABLE (string path, length, md5, mimetype, encoding, mtime)"
#"CREATE TABLE (string path, camera_make, camera_model, date_taken)"
#"CREATE TABLE (string path)"

DB="fileinfo.db"
  
def compute_pic_info(full_path):
  metadata = pyexiv2.metadata.ImageMetadata(full_path)
  try:
    metadata.read()
  except IOError as e:
    print e
    return None
  mimetype = metadata.mime_type
  
  if "Exif.Image.Make" not in metadata.exif_keys or "Exif.Image.Model" not in metadata.exif_keys or "Exif.Photo.DateTimeOriginal" not in metadata.exif_keys:
    return None
  return [metadata["Exif.Image.Make"].value,
          metadata["Exif.Image.Model"].value,
          metadata["Exif.Photo.DateTimeOriginal"].value]

db = sqlite3.connect(sys.argv[1])

c = db.cursor()
c.execute("SELECT mimetype, path FROM file_info WHERE mimetype='image/jpeg'")

for mimetype, full_path in c:
  print full_path
  c2 = db.cursor()
  c2.execute("SELECT path FROM pic_info WHERE path = ?", (full_path, ))
  if mimetype in ["image/jpeg"] and len(c2.fetchall()) == 0:
    pic_info = compute_pic_info(full_path)
    if pic_info:
      print " ".join([str(x) for x in pic_info])
      db.execute("insert into pic_info values (?, ?, ?, ?)", [full_path] + pic_info)
      db.commit()
db.close()