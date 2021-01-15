build:
	env GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o bin/ofclient-nix-amd64
	env GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o bin/ofclient-nix-arm64
	env GOOS=windows GOARCH=amd64 go build -o bin/ofclient-win-64.exe 
	env GOOS=windows GOARCH=386 go build -o bin/ofclient-win-32.exe
	env GOOS=darwin GOARCH=amd64 go build -o bin/ofclient-osx 