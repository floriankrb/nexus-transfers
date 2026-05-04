
we want to, optionally, use s3 as a staging space, so when copying a file, the initiater will call get_file with option use_s3 (add this option to the nexus-copy cli).

let's call clientA the initiator, typically using nexus-copy.
let's call clientB the data provider, typically using nexus-client.

we want
clientA sends a get_file command to clientB with option use_s3
clientB upload the file to an s3 bucket $NEXUS_TRANSFER_S3_BUCKET
clientB returns the s3 path of the file that have been uploaded, **including the bucket name**, to clientA
clientA download the file from S3 using the bucket name received from clientB
clientA tells clientB that the file has been downloaded
clientB delete the file from S3 (do not delete it from disk)

Note: clientA (the receiving side) does **not** need `NEXUS_TRANSFER_S3_BUCKET` — the bucket name is returned by clientB in the reply.

please use obstore python package.

for the credentials, use
endpoint_url = $NEXUS_TRANSFER_S3_ENDPOINT_URL
access_key_id = $NEXUS_TRANSFER_S3_ACCESS_KEY_ID
secret_access_key = $NEXUS_TRANSFER_S3_SECRET_ACCESS_KEY

write also a skill for this


when pushing file to s3, create a directory below the bucket, with the following format:
YYYY-MM-DD-HHMMSS-<source>-<target>-<random-uuid>
this directory name is provided by the nexus-copy so that all file from the same transfer are in the sub-"folder", the rest of the file path is made of the full path name of the file.
the date is UTC


nexus-copy should have the option --site which will be used (if given) instead of the word 'copy' in the generated name.
add also an option --size, to show the progress in bytes instead of number of files, only ask for the remote sizes if this option is set.