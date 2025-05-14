import paramiko
import os

# --- CONFIG ---
hostname = "44.217.138.250"
port = 22
username = "ec2-user"  # change if you create a different SFTP user
key_path = "/Users/sandeepmalkanoor/Downloads/sftp-key.pem"  # <- update to correct path
remote_dir = "/home/hoteluser/uploads"  # <- or wherever your PDFs are uploaded
local_dir = "/Users/sandeepmalkanoor/Documents/Python/data"  # <- change to your Mac ETL folder

# --- Prepare local directory ---
os.makedirs(local_dir, exist_ok=True)

# --- Load private key ---
key = paramiko.RSAKey.from_private_key_file(key_path)

# --- Connect using key ---
transport = paramiko.Transport((hostname, port))
transport.connect(username=username, pkey=key)
sftp = paramiko.SFTPClient.from_transport(transport)

# --- Download PDFs ---
for filename in sftp.listdir(remote_dir):
    if filename.endswith(".pdf"):
        local_path = os.path.join(local_dir, filename)
        remote_path = f"{remote_dir}/{filename}"

        if not os.path.exists(local_path):
            print(f"⬇️ Downloading: {filename}")
            sftp.get(remote_path, local_path)

# --- Cleanup ---
sftp.close()
transport.close()

print("✅ All PDFs downloaded.")
