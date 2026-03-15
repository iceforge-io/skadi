tarball:
	tar \
	  --exclude-vcs \
	  --exclude='*/target' \
	  --exclude='*/target/*' \
	  --exclude='*/node_modules' \
	  --exclude='*.log' \
	  -cvzf /mnt/c/Users/dkay2/Downloads/skadi.tar.gz .

issues:
	gh issue list \
	  --repo iceforge-io/skadi \
	  --limit 200 \
	  --json number,title,body,labels,assignees,state \
	  > /mnt/c/Users/dkay2/Downloads/skadi-issues.json
