#!/bin/bash
# Purpose: CLI program to check and operate LambdaRedshiftLoader batches
# Author: Andrey Suvorov
# Created: 2021-09-14
# Updated: 2021-11-02

SCRIPT_NAME=$0

usage(){
    echo ""
    echo "usage: $SCRIPT_NAME [-arpg] [-a account] [-r role] [-p profile] [-g region] [-u 1]"
    echo "arguments: "
    echo "  -a      aws account id"
    echo "  -r      aws role"
    echo "  -p      aws default profile"
    echo "  -g      aws region"
    echo "  -u      (1) show usage"
    echo ""
    exit 1
}

# get arguments
while getopts ":a:r:p:g:u:" opt; do
  case $opt in
    a) AWS_ACCOUNT_ID="$OPTARG"
      ;;
    r) AWS_ROLE="$OPTARG"
      ;;
    p) AWS_PROFILE="$OPTARG"
      ;;
    g) AWS_REGION="$OPTARG"
      ;;
    u)
      usage
      ;;
    :)
      echo "$SCRIPT_NAME: missing value for -$OPTARG argument" >&2
      usage
      ;;
    \?)
      echo "$SCRIPT_NAME: invalid argument -$OPTARG provided" >&2
      usage
      ;;
  esac
done

# set default arguments here:
if [ "$AWS_ACCOUNT_ID" == "" ]; then AWS_ACCOUNT_ID="XXXXXXXXXXX"; fi
if [ "$AWS_ROLE" == "" ]; then AWS_ROLE="Developer"; fi
if [ "$AWS_PROFILE" == "" ]; then AWS_PROFILE="default"; fi
if [ "$AWS_REGION" == "" ]; then AWS_REGION="us-west-2"; fi

menuHeader(){
  clear
  echo " "
  echo "*****************************************************************************************"
  echo "*                                      $1                                      *"
  echo "*****************************************************************************************"
}

displayMsg(){
  echo " "
  echo "---------------------------------------- MESSAGE ----------------------------------------"
  echo " $1"
  echo "-----------------------------------------------------------------------------------------"
  echo " "
}

getCredentials(){
  local l_retryLogin
  menuHeader " AWS Login "
  displayMsg "AWS account: $AWS_ACCOUNT_ID\n AWS Region: $AWS_REGION\n AWS Role: $AWS_ROLE\n AWS Profile: $AWS_PROFILE"
  while ! eiamcli getAWSTempCredentials -a "$AWS_ACCOUNT_ID" -r "$AWS_ROLE" -p "$AWS_PROFILE" ; do
    if ! eiamcli login; then
      displayMsg "Can't authenticate the user. Exiting program."
      read -r -p "Would you like to try login again? (y/n): " l_retryLogin
      if ! [ "$l_retryLogin" == 'y' ]; then
        displayMsg "Exiting program"
        exit 1
      fi
    else
      eiamcli getAWSTempCredentials -a "$AWS_ACCOUNT_ID" -r "$AWS_ROLE" -p "$AWS_PROFILE"
    fi
  done
}

inputDate(){
  local dt=""
  while ! [[ "$dt" =~ ^[0-1][0-9]/[0-3][0-9]/[0-9]{4}$ && $(date -j -f %m/%d/%Y "$dt"  +%s) ]]; do
    if ! [[ "$dt" == "" ]]; then displayMsg "Invalid date format for $1 : $dt" > /dev/tty; fi
    read -r -p "Please enter $1 (MM/DD/YYYY): " dt
  done
  echo "$dt"
}

batchAction(){
  local l_batchStatus=$1
  local l_response=$2

  batchCount=$(echo "$2" | jq '. | length')
  action_menu=true
  action_executed=""
  menuHeader "Action Menu"

  while [[ "$action_menu" == true ]]; do
    if [[ "$action_executed" == "" ]]; then
      displayMsg "Found $batchCount batches with '$l_batchStatus' status from $startDate to $endDate"
    else
      displayMsg "$action_executed batches command completed"
      action_executed=""
    fi
    PS3="Please select the action from the menu above: "
    select action in "${actions[@]}"; do
      case $action in
        "List")
          displayMsg "$action batches with $l_batchStatus status"
          echo "$l_response" | jq
          action_executed=$action
          break
          ;;
        "Describe")
          displayMsg "$action batches with $l_batchStatus status"
          counter=1

          for row in $(echo "$l_response" | jq -r '.[] | @base64'); do
            _jq() {
                  echo "${row}" | base64 --decode | jq -r "${1}"
                  }

            # exit individual items action menu
            if ! [[  "$action_executed" == "" ]]; then break; fi

            batchId=$(_jq '.batchId')
            s3Prefix=$(_jq '.s3Prefix')

            echo "==> Describe batch $counter of $batchCount (batchId: ${batchId} s3Prefix: ${s3Prefix})"

            node describeBatch.js \
                  --region "${AWS_REGION}" \
                  --batchId "${batchId}" \
                  --s3prefix "${s3Prefix}" | jq

            echo ""

            counter=$((counter+1))

            # select an individual action for each batch in the query, one by one
            PS3="Please select the batch action from the menu above: "
            if [[ "$l_batchStatus" == "error" || "$l_batchStatus" == "locked" ]]; then
              batch_actions=("Reprocess" "Unlock" "Delete" "Next" "Exit")
            elif [[ "$l_batchStatus" == "complete" ]]; then
              batch_actions=("Next" "Delete" "Exit")
            elif [[ "$l_batchStatus" == "open" ]]; then
              batch_actions=("Next" "Exit")
            fi

            select batch_action in "${batch_actions[@]}"; do
              case $batch_action in
                "Reprocess")
                  echo "==> Reprocessing (batchId: ${batchId} s3Prefix: ${s3Prefix})"
                  node reprocessBatch.js \
                        --region "${AWS_REGION}" \
                        --batchId "${batchId}" \
                        --prefix "${s3Prefix}"

                  break
                  ;;
                "Unlock")
                  echo "==> Unlocking (batchId: ${batchId} s3Prefix: ${s3Prefix})"
                  node unlockBatch.js \
                        "${AWS_REGION}" \
                        "${batchId}" \
                        "${s3Prefix}"
                  break
                  ;;
                "Delete")
                  echo "==> Deleting (batchId: ${batchId} s3Prefix: ${s3Prefix})"
                  node deleteBatch.js \
                        --region "${AWS_REGION}" \
                        --batchId "${batchId}" \
                        --s3Prefix "${s3Prefix}"
                  break
                  ;;
                "Next")
                  echo "==> Next batch"
                  break
                  ;;
                "Exit")
                  clear
                  action_executed=$action
                  break
                  ;;
                *) displayMsg "Invalid option selected $REPLY"
                  ;;
              esac
            done
          done
          action_executed=$action
          break
          ;;
        "Reprocess")
          displayMsg "$action batches with $l_batchStatus status"
          counter=1
          for row in $(echo "$l_response" | jq -r '.[] | @base64'); do
            _jq() {
                  echo "${row}" | base64 --decode | jq -r "${1}"
                  }

            batchId=$(_jq '.batchId')
            s3Prefix=$(_jq '.s3Prefix')

            echo "==> Reprocessing batch $counter of $batchCount (batchId: ${batchId} s3Prefix: ${s3Prefix})"

            node reprocessBatch.js \
                  --region "${AWS_REGION}" \
                  --batchId "${batchId}" \
                  --prefix "${s3Prefix}"

            counter=$((counter+1))
          done
          action_executed=$action
          break
          ;;
        "Unlock")
          displayMsg "$action batches with $l_batchStatus status"
          counter=1
          for row in $(echo "$l_response" | jq -r '.[] | @base64'); do
            _jq() {
                  echo "${row}" | base64 --decode | jq -r "${1}"
                  }

            batchId=$(_jq '.batchId')
            s3Prefix=$(_jq '.s3Prefix')

            echo "==> Unlocking batch $counter of $batchCount (batchId: ${batchId} s3Prefix: ${s3Prefix})"

            node unlockBatch.js \
                  "${AWS_REGION}" \
                  "${batchId}" \
                  "${s3Prefix}"

            counter=$((counter+1))
          done
          action_executed=$action
          break
          ;;
        "Delete")
          displayMsg "$action batches with $l_batchStatus status from $startDate to $endDate"

          node deleteBatches.js \
                --region "${AWS_REGION}" \
                --startDate "${startDateUnix}" \
                --endDate "${endDateUnix}" \
                --batchStatus "$l_batchStatus" \
                --dryRun false
          action_executed=$action
          break
          ;;
        "Main Menu")
          clear
          action_menu=false
          break
          ;;
        *) displayMsg "Invalid option selected $REPLY"
          ;;
      esac
    done
  done
}

queryBatch(){
  response=$(node queryBatches.js \
                  --region "${AWS_REGION}" \
                  --batchStatus "$1" \
                  --startDate "${startDateUnix}" \
                  --endDate "${endDateUnix}" \
            )

  echo "$response"
}

main(){
  startDate=$(date -v-7d '+%m/%d/%Y')
  startDateUnix=$(date -j -f %m/%d/%Y "$startDate"  +%s)
  endDate=$(date '+%m/%d/%Y')
  endDateUnix=$(date -j -f %m/%d/%Y "$endDate"  +%s)
  dates_set=false

  getCredentials

  main_menu=true
  while [ "$main_menu" == true ]; do

    menuHeader " Main Menu "

    if ! [[ "$startDate" == "" && "$endDate" == "" ]]; then
      if [[ "$dates_set" == false  ]]; then
        read -r -p "Do you want to use selected dates to query batches? $startDate to $endDate (y/n): " defaultDates
        if ! [[ $defaultDates =~ ^[Yy]$ ]]; then
          startDateUnix=0
          endDateUnix=0
          while ! [[ "$startDateUnix" < "$endDateUnix" ]]; do
            startDate=$(inputDate 'Start Date')
            startDateUnix=$(date -j -f %m/%d/%Y "$startDate"  +%s)
            endDate=$(inputDate 'End Date')
            endDateUnix=$(date -j -f %m/%d/%Y "$endDate"  +%s)
            if ! [[ "$startDateUnix" < "$endDateUnix" ]]; then
              displayMsg "The Start Date $startDate can't be greater than End Date $endDate";
            fi
          done
        fi
        dates_set=true
      fi
    fi

    menuHeader " Main Menu "
    if ! [[ $outputMsg == "" ]]; then
      displayMsg "$outputMsg"
      outputMsg=""
    else
      displayMsg "Query dates set from Start Date $startDate to End Date $endDate\n Please use option 6 to change dates"
    fi

    PS3="What you would like to do? Please select from the menu: "
    statuses=("Check Error batches" "Check Locked batches" "Check Open batches"
              "Check Complete batches" "Check Other batches" "Change query dates" "Exit")
    select status in "${statuses[@]}"; do
      case $status in
        "Check Error batches")
          queryResult=$(queryBatch "error")
          if ! [[ "$queryResult" == "[]" ]]; then
            actions=("List" "Describe" "Reprocess" "Delete" "Main Menu")
            batchAction "error" "$queryResult"
          else
            outputMsg="No batches with status 'error' found for specified dates $startDate - $endDate"
          fi
          break
          ;;
        "Check Locked batches")
          queryResult=$(queryBatch "locked")
          if ! [[ "$queryResult" == "[]" ]]; then
            actions=("List" "Describe" "Unlock" "Reprocess" "Delete" "Main Menu")
            batchAction "locked" "$queryResult"
          else
            outputMsg="No batches with status 'locked' found for specified dates $startDate - $endDate"
          fi
          break
          ;;
        "Check Open batches")
          queryResult=$(queryBatch "open")
          if ! [[ "$queryResult" == "[]" ]]; then
            actions=("List" "Describe" "Main Menu")
            batchAction "open" "$queryResult"
          else
            outputMsg="No batches with status 'open' found for specified dates $startDate - $endDate"
          fi
          break
          ;;
        "Check Complete batches")
          queryResult=$(queryBatch "complete")
          if ! [[ "$queryResult" == "[]" ]]; then
            actions=("List" "Describe" "Delete" "Main Menu")
            batchAction "complete" "$queryResult"
          else
            outputMsg="No batches with status 'complete' found for specified dates $startDate - $endDate"
          fi
          break
          ;;
        "Check Other batches")
          read -r -p "Please type in batch status to query: " batchStatus
          queryResult=$(queryBatch "$batchStatus")
          if ! [[ "$queryResult" == "[]" ]]; then
            actions=("List" "Describe" "Delete" "Main Menu")
            batchAction "$batchStatus" "$queryResult"
          else
            outputMsg="No batches with status '$batchStatus' found for specified dates $startDate - $endDate"
          fi
          break
          ;;
        "Change query dates")
          dates_set=false
          clear
          break
          ;;
        "Exit")
          displayMsg "Exiting program"
          exit
          ;;
        *)
          displayMsg "Invalid option selected $REPLY"
          ;;
      esac
    done
  done
}

main