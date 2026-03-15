tarball:
	tar \
	  --exclude-vcs \
	  --exclude='*/target' \
	  --exclude='*/target/*' \
	  --exclude='*/node_modules' \
	  --exclude='*.log' \
	  -cvzf /mnt/c/Users/dkay2/Downloads/skadi.tar.gz .