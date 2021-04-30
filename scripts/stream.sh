#!/bin/sh

# >>> conda initialize >>>
__conda_setup="$($HOME/miniconda3/bin/conda 'shell.bash' 'hook' 2> /dev/null)"
if [ $? -eq 0 ]; then
    eval "$__conda_setup"
else
    if [ -f "$HOME/miniconda3/etc/profile.d/conda.sh" ]; then
        . "$HOME/miniconda3/etc/profile.d/conda.sh"
    else
        export PATH="$HOME/miniconda3/bin:$PATH"
    fi
fi
unset __conda_setup
# <<< conda initialize <<<

conda activate ws37
env | grep CONDA

echo "$@"
conda list  "^ws|^ml|^pytorch|^python$"
scripts/driver.py "$@"
