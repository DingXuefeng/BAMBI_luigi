#!/bin/bash

export PYTHONPATH=$(readlink -f .)

luigi --module BAMBI_luigi EasySendReport
