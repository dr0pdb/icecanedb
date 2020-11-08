VERSION?="0.0.1"
PKG="github.com/dr0pdb/icecanedb"

compile:
	GOOS=freebsd GOARCH=386 go build $(PKG)
	GOOS=linux GOARCH=386 go build $(PKG)
	GOOS=windows GOARCH=386 go build $(PKG)