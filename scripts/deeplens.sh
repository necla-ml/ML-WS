#!/bin/sh

# >>> conda initialize >>>
__conda_setup="$('/home/aws_cam/miniconda3/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
if [ $? -eq 0 ]; then
    eval "$__conda_setup"
else
    if [ -f "/home/aws_cam/miniconda3/etc/profile.d/conda.sh" ]; then
        . "/home/aws_cam/miniconda3/etc/profile.d/conda.sh"
    else
        export PATH="/home/aws_cam/miniconda3/bin:$PATH"
    fi
fi
unset __conda_setup
# <<< conda initialize <<<

conda activate ${ENV:-$CONDA_DEFAULT_ENV}
env | grep CONDA

conda list  "^ws|^ml|^pytorch|^python$"
scripts/`basename $0 | cut -d. -f1`.py "$@"