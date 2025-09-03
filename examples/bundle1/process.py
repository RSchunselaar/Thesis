import subprocess, os

subprocess.run(["bash", "./post.sh"])  # should be picked up
os.system("sh -c 'echo done'")  # not a script edge
