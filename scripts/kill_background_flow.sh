#!/bin/bash

ps aux | grep iperf | awk '{print $2}' | xargs -I {} kill {}
