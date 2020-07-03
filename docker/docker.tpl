#!/bin/bash

if [[ "$1" == "image" ]] && [[ "$2" == "build" ]]; then
    #this command is to build a docker image.
    #repleace the docker file with modified docker file for image build
    args=( "$@" )
    build_path=""
    original_dockerfile=""
    imageid=""
    for ((i=0; i < $#; i++)) ;do
        if [[ "${args[$i]}" == "-f" ]]; then
            i=$((i+1))
            original_dockerfile=${args[$i]}
        elif [[ ${args[$i]} == --file=* ]]; then
            original_dockerfile="$(echo "${args[$i]}" | cut -d "=" -f 2)"
        elif [[ "${args[$i]}" == "-t" ]]; then
            i=$((i+1))
            imageid=${args[$i]}
        elif [[ ${args[$i]} == --tag=* ]]; then
            imageid="$(echo "${args[$i]}" | cut -d "=" -f 2)"
        elif [[ $((i+1)) -eq $# ]]; then
            build_path=${args[$i]}
        fi
    done
    if [[ "$original_dockerfile" == "" ]]; then
        echo "Can't find docker file from command line,skip meta data collection"
        set "${args[@]}"
        __DOCKER__ "$@"
    elif [[ "$build_path" == "" ]]; then
        echo "Can't find build path from command line,skip meta data collection"
        set "${args[@]}"
        __DOCKER__ "$@"
    elif [[ "$imageid" == "" ]]; then
        echo "Can't find image id from command line,skip meta data collection"
        set "${args[@]}"
        __DOCKER__ "$@"
    else
        dockerfile=$(__WORKDIR__/docker_prebuild "$(pwd)" "${build_path}" "${original_dockerfile}")
        if [[ $? -eq 0  ]]; then
            for ((i=0; i < $#; i++)) ;do
                if [[ "${args[$i]}" == "-f" ]]; then
                    i=$((i+1))
                    original_dockerfile=args[$i]
                    args[$i]="$dockerfile"
                elif [[ ${args[$i]} == --file=* ]]; then
                    args[$i]="--file=${dockerfile}"
                fi
            done
            set "${args[@]}"
            # verify that it worked
            __DOCKER__ "$@"
            if [[ $? -eq 0 ]]; then
                #harvest the data and push them to blob storage
                __WORKDIR__/docker_harvest "${imageid}"
            fi
            rm -rf $(dirname ${dockerfile})
        else
            echo "Call prebuild failed.skip meta data collection"
            set "${args[@]}"
            __DOCKER__ "$@"
        fi
    fi
else
    __DOCKER__ "$@"
fi

