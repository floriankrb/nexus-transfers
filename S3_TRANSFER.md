
we want to, optionally, use s3 as a staging space, so when copying a file, the initiater will call get_file with option use_s3 (add this option to the nexus-copy cli).

let's call clientA the initiator, typically using nexus-copy.
let's call clientB the data provider, typically using nexus-client.

we want
clientA sends a get_file command to clientB with option use_s3
clientB upload the file to an s3 bucket $NEXUS_TRANSFER_S3_BUCKET
clientB returns the s3 path of the file that have been uploaded, to clientA
clientA download the file from S3
clientA tells clientB that the file has been downloaded
clientB delete the file from S3 (do not delete it from disk)

please use obstore python package.

for the credentials, use
endpoint_url = $NEXUS_TRANSFER_S3_ENDPOINT_URL
access_key_id = $NEXUS_TRANSFER_S3_ACCESS_KEY_ID
secret_access_key = $NEXUS_TRANSFER_S3_SECRET_ACCESS_KEY

write also a skill for this