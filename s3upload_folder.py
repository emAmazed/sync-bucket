# s3upload_folder.py
# Can be used recursive file upload to S3.
# If bucket is not in S3, it will be created.
# It will create mirrorred file structure in S3.
# The folder to upload should be located at current working directory.

import boto

import os
import sys
import argparse

#max size in bytes before uploading in parts. between 1 and 5 GB recommended
MAX_SIZE = 20 * 1000 * 1000
#size of parts when uploading in parts
PART_SIZE = 6 * 1000 * 1000

def check_arg(args=None):
  parser = argparse.ArgumentParser(description='args : start/start, instance-id')
  parser.add_argument('-b', '--bucket',
      help='bucket name',
      required='True',
      default='')
  parser.add_argument('-f', '--foldername',
      help='folder name to upload',
      required='True',
      default='')

  results = parser.parse_args(args)
  return (results.bucket,
      results.foldername)


def percent_cb(complete, total):
  sys.stdout.write('.')
  sys.stdout.flush()


def upload_to_s3(bucket_name, source):
  print 'source=',source
  print 'bucket_name=',bucket_name
  aws_access_key_id = boto.config.get('Credentials', 'aws_access_key_id')
  aws_secret_access_key = boto.config.get('Credentials', 'aws_secret_access_key')

  conn = boto.connect_s3(aws_access_key_id, aws_secret_access_key)

  # check if the bucket exists
  nonexistent = conn.lookup(bucket_name)
  if nonexistent is None:
    print 'No such bucket!'
    print 'Creating %s bucket' %(bucket_name)
    bucket = conn.create_bucket(bucket_name,
        location=boto.s3.connection.Location.DEFAULT)
  else:
    bucket = conn.get_bucket(bucket_name, validate=True)

  print 'bucket=%s' %(bucket)

  # construct the upload file list
  uploadFileNames = []
  for root, dirs, files in os.walk(source, topdown=False):
    for name in files:
      fname=os.path.join(root, name)
      print fname
      uploadFileNames.append(fname)
  print 'uploadFileNames = ', uploadFileNames

  # start uploading
  for filename in uploadFileNames:
    print 'filename=',filename
    sourcepath = filename
    destpath = filename
    print 'Uploading %s to Amazon S3 bucket %s' %(sourcepath, bucket_name)

    filesize = os.path.getsize(sourcepath)
    if filesize > MAX_SIZE:
      print "multipart upload"
      mp = bucket.initiate_multipart_upload(destpath)
      fp = open(sourcepath,'rb')
      fp_num = 0
      while (fp.tell() < filesize):
        fp_num += 1
        print "uploading part %i" %fp_num
        mp.upload_part_from_file(fp, fp_num, cb=percent_cb, num_cb=10, size=PART_SIZE)

      mp.complete_upload()

    else:
      print "singlepart upload"
      k = boto.s3.key.Key(bucket)
      k.key = destpath
      k.set_contents_from_filename(sourcepath, cb=percent_cb, num_cb=10)


if __name__ == '__main__':
  '''
  Usage:
  python s3upload_folder.py -b s3-sample-bucket -f sample-folder
  python s3upload_folder.py -b s3-sample-bucket -f sample-folder/sample-file
  python s3upload_folder.py -b elearn-repo -f d3
  '''

  bucket, foldername = check_arg(sys.argv[1:])
  upload_to_s3(bucket, foldername)

