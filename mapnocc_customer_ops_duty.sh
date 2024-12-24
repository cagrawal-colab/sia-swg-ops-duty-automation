#!/bin/bash
#
# Speed up Mapnocc Duty and add a comment with all required links
#
# If you see curl errors, for example:
# curl: (58) SSL: Can't load the certificate "/Users/ljankows/.certs/cagrawal.crt" 
#
# please update your curl to at least: 7.64.1
#
######

NETWORK=nevada

# fail on all errors
set -o errexit
set -o pipefail

#
# functions
#

# show help
show_help()
{
  cat << EOF
Usage: $0  <jira-ticket> 
Parameters:
  jira-ticket           Jira ticket to add the comment to
EOF
}

# show error message
# $1 - message
show_err()
{
    printf '\033[01;31mERROR\033[00m %s\n' "$1" >&2
}

# show informative message
# $1 - message
show_msg()
{
    printf '\033[01;32mINFO\033[00m %s\n' "$1"
}

# die - end script with error
# $1 - error message
die()
{
    show_err "$1"
    exit 1
}

# execute curl command
# $@ - curl options
do_curl()
{
    curl \
        --cert ~/.certs/"$USER.crt" \
        --key ~/.certs/"$USER.key" \
        -H 'Accept: application/json' \
        -H 'Content-Type: application/json' \
        --fail \
        --silent \
        --show-error \
        "$@"
}

# prepare a Jira comment body
prepare_comment()
{
    
    cat << EOF
    $(cat customer_drops.txt)
EOF
}

# post jira comment
# $1 - ticket number
# $2 - body of the comment
post_comment()
{
    local ticket=$1
    local comment=$2
    local post_body=$(sed -n -e 'H;${x;s/\n/\\n/g;s/^\\n//;p;}' <<< "{\"body\": \"$comment\"}")

    do_curl --data "$post_body" "https://track-api.akamai.com/jira/rest/api/2/issue/$ticket/comment" \
      || return 1

    # extra echo is cosmetic but required as Jira API output does not include last \n
    echo
}


#
# main
#

if [[ $# -ne 1 ]]; then
    show_err "Invalid number of parameters!"
    show_help
    exit 1
fi
ticket=$1
show_msg "Preparing the comment..."
comment=$(prepare_comment)

show_msg "Posting comment to $ticket..."
if ! post_comment "$ticket" "$comment"; then
    die "Could not post comment to Jira!"
fi

show_msg "All Done"
exit 0

# end of script
