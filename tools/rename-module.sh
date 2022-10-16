if [[ "$1" == "" ]]; then
    echo "no new module name specified"
    exit 1
fi

NEW_MODULE_NAME="github.com/Thospol/$1"

test -e "$(go env GOPATH)/bin/gomajor" || go install github.com/icholy/gomajor@latest
# shellcheck disable=SC2093
exec "$(go env GOPATH)/bin/gomajor" path "$NEW_MODULE_NAME"
