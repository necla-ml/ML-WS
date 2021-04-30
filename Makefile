ENV?=ws37
ifndef SVC
ifeq ($(USER), aws_cam)
	SVC:=deeplens
endif
endif

SVC?=kvs # nuuo not default anymore
DRIVER:=scripts/$(SVC).sh
ROUTINE:=scripts/$(SVC).py
SERVICE:=scripts/$(SVC).service
user ?= $(USER)
ifeq ($(SVC), deeplens)
	serial=$(shell sudo dmidecode -s system-serial-number)
	serial:=$(lastword $(subst -, ,$(serial)))
else ifeq ($(SVC), nuuo)
	NUUO_SITE_ID ?= 3
	#area?=First Floor Books
	#area?=Second Floor Fisheye
	#area?=First Floor Reg. 1 and 2
	#fps ?= 15
	#fps ?= 8
	
	#NUUO_SITE_ID ?= 100
	#area?=Highway
	#fps ?= 30
	
	NUUO_SITE_ID ?= 873
	area?= Cam 1
	fps ?= 10
	
	stream?=loopback
else ifeq ($(SVC), kvs)
	# local file or remote URL
	stream?=loopback
	fps ?= 10
endif

## VCS

.PHONY: clone checkout co pull build package install

require-version:
ifndef version
	$(error version is undefined)
endif

clone:
	git clone --recursive $(url) $(dest)

checkout:
	git submodule update --init --recursive

co: checkout

pull: co
	git pull

merge:
	git checkout main
	git merge dev
	git push

tag: require-version
	git checkout main
	git tag -a v$(version) -m v$(version)
	git push origin tags/v$(version)

del-tag:
	git tag -d $(tag)
	git push origin --delete tags/$(tag)

release:
	git checkout $(git describe --abbrev=0 --tags)

## Environment

conda-install:
	wget -O $(HOME)/Downloads/Miniconda3-latest-Linux-x86_64.sh https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
	sh $(HOME)/Downloads/Miniconda3-latest-Linux-x86_64.sh -b -p $(HOME)/miniconda3
	rm -fr $(HOME)/Downloads/Miniconda3-latest-Linux-x86_64.sh

conda-setup:
	echo '' >> $(HOME)/.bashrc
	echo eval \"'$$('$(HOME)/miniconda3/bin/conda shell.bash hook')'\" >> $(HOME)/.bashrc
	echo conda activate $(ENV) >> $(HOME)/.bashrc
	echo '' >> $(HOME)/.bashrc
	echo export EDITOR=vim >> $(HOME)/.bashrc
	echo export PYTHONDONTWRITEBYTECODE=1 >> $(HOME)/.bashrc

conda: conda-install conda-setup
	echo Restart your shell to create and activate conda environment "$(ENV)"

dep-linux:	# opencv on libGL.so
	sudo apt update && sudo apt install -y \
        libgl1-mesa-glx

dep-osx:
	brew install pkg-config openssl cmake gstreamer gst-plugins-base gst-plugins-good gst-plugins-bad gst-plugins-ugly log4cplus gst-libav

## Conda Distribution

conda-build:
	conda config --set anaconda_upload yes
	conda-build purge-all
	GIT_LFS_SKIP_SMUDGE=1 conda-build --user NECLA-ML recipe

conda-clean:
	conda clean --all

## SDK and Standalone Package

kvs3-cffi:
	cd ml/csrc; ./cdef src/cdef.c

clean:
	python setup.py clean --all
	@rm -fr ml/csrc/src/*.h
	@rm -fr dist

## Local Development

dev:
	git config --global credential.helper cache --timeout=21600
	git checkout dev
	make co

dev-setup: dev kvs3-cffi
	pip install -vv --no-deps --ignore-installed --no-build-isolation --no-binary :all: -e .

uninstall-develop:
	pip uninstall ML-WS

## Systemd Service

deploy:
ifeq ($(SVC), deeplens)
	@echo Deploying $(SVC): $(user)-$(serial)
else
	@echo Deploying $(SVC) streaming producer
endif
	echo '[Unit]' > $(SERVICE)
	echo 'Description=Web Service for Kinesis Video Streams' >> $(SERVICE)
	echo '' >> $(SERVICE)
	echo '[Service]' >> $(SERVICE)
	echo 'Type=simple' >> $(SERVICE)
	echo "RemainAfterExit=no" >> $(SERVICE)
	echo "Restart=always" >> $(SERVICE)
	echo "RestartSec=5s" >> $(SERVICE)
	echo "WorkingDirectory=$(PWD)" >> $(SERVICE)
	echo "Environment=HOME=$(HOME)" >> $(SERVICE)
	echo "Environment=ENV=$(ENV)" >> $(SERVICE)
	echo "Environment=PYTHONDONTWRITEBYTECODE=1" >> $(SERVICE)
	echo "Environment=AWS_DEFAULT_REGION=us-east-1" >> $(SERVICE)
ifeq ($(SVC), deeplens)
	echo "ExecStart=$(PWD)/$(DRIVER) -u $(user) --stream $(serial) --fps 15">> $(SERVICE)
else ifeq ($(SVC), nuuo)
	echo "ExecStart=$(PWD)/$(DRIVER)" >> $(SERVICE) --stream \"$(stream)\" --area \"$(area)\" --fps $(fps) --site_id $(NUUO_SITE_ID) --env PROD
else ifeq ($(SVC), kvs)
ifndef path
	$(error 'path' to a bitstream file or remote URL must be specified)
endif
	echo "ExecStart=$(PWD)/$(DRIVER)" >> $(SERVICE) \"$(path)\" --fps $(fps) --stream \"$(stream)\" --loop
endif
	echo '' >> $(SERVICE)
	echo '[Install]' >> $(SERVICE)
	echo 'WantedBy=multi-user.target' >> $(SERVICE)
	echo 'Alias=$(SVC).service' >> $(SERVICE)
	sudo ln -sf `realpath $(SERVICE)` /lib/systemd/system/$(SVC).service
	sudo systemctl daemon-reload
	sudo systemctl enable $(SVC).service

remove:
	sudo systemctl stop $(SVC)
	sudo systemctl disable $(SVC)
	sudo systemctl daemon-reload
	sudo systemctl reset-failed

start:
	sudo systemctl start $(SVC)

stop:
	sudo systemctl stop $(SVC)

restart:
	sudo systemctl restart $(SVC)

reload:
	sudo systemctl daemon-reload

status:
	sudo systemctl status $(SVC)

log:
	journalctl -u $(SVC) -r

start-broker:
	docker run --rm --name activemq -d -p 61616:61616 -p 8161:8161 -p 61613:61613 rmohr/activemq
stop-broker:
	docker stop activemq
restart-broker:
	docker restart activemq