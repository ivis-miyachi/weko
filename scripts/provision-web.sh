#!/usr/bin/env bash
#
# This file is part of WEKO3.
# Copyright (C) 2017 National Institute of Informatics.
#
# WEKO3 is free software; you can redistribute it
# and/or modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# WEKO3 is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with WEKO3; if not, write to the
# Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston,
# MA 02111-1307, USA.

# quit on errors:
set -o errexit

# check environment variables:
if [ "${INVENIO_WEB_VENV}" = "" ]; then
    echo "[ERROR] Please set environment variable INVENIO_WEB_VENV before runnning this script."
    echo "[ERROR] Example: export INVENIO_WEB_VENV=invenio"
    exit 1
fi

# quit on unbound symbols:
set -o nounset

# detect pathname of this script:
scriptpathname=$(cd "$(dirname "$0")" && pwd)

# sphinxdoc-install-detect-sudo-begin
# runs as root or needs sudo?
if [[ "$EUID" -ne 0 ]]; then
    sudo='sudo'
else
    sudo=''
fi
# sphinxdoc-install-detect-sudo-end

# unattended installation:
export DEBIAN_FRONTEND=noninteractive


provision_web_common_centos7 () {

    # sphinxdoc-install-useful-system-tools-centos7-begin
    # add EPEL external repository:
    $sudo yum install -y epel-release

    # install useful system tools:
    $sudo yum install -y \
         curl \
         git \
         rlwrap \
         screen \
         vim
    # sphinxdoc-install-useful-system-tools-centos7-end

    # sphinxdoc-add-nodejs-external-repository-centos7-begin
    if [[ ! -f /etc/yum.repos.d/nodesource-el.repo ]]; then
        curl -sL https://rpm.nodesource.com/setup_20.x | $sudo bash -
    fi
    # sphinxdoc-add-nodejs-external-repository-centos7-end

    # sphinxdoc-install-web-common-centos7-begin
    # install development tools:
    $sudo yum update -y
    $sudo yum groupinstall -y "Development Tools"
    $sudo yum install -y \
         libffi-devel \
         libxml2-devel \
         libxslt-devel \
         openssl-devel \
         libzip-devel \
         libjpeg-turbo-devel \
         policycoreutils-python \
         python-devel \
         python-pip
    $sudo yum install -y --disablerepo=epel \
         nodejs
    # sphinxdoc-install-web-common-centos7-end

}

provision_web_libpostgresql_centos7 () {

    # sphinxdoc-install-web-libpostgresql-centos7-begin
    $sudo yum install -y \
         postgresql-devel
    # sphinxdoc-install-web-libpostgresql-centos7-end
}

setup_npm_and_css_js_filters () {
    # sphinxdoc-install-npm-and-css-js-filters-begin
    # $sudo su -c "npm install -g npm"
    $sudo su -c "npm install -g node-sass@9.0.0 clean-css@3.4.12 requirejs uglify-js"
    # sphinxdoc-install-npm-and-css-js-filters-end

}

setup_virtualenvwrapper () {
    # disable quitting on errors due to virtualenvrapper:
    set +o errexit
    set +o nounset

    # sphinxdoc-install-virtualenvwrapper-begin
    #$sudo pip install -U setuptools==71.0.1 pip==24.1.2
    $sudo pip install -U setuptools pip
    $sudo pip install -U virtualenvwrapper
    if ! grep -q virtualenvwrapper ~/.bashrc; then
        mkdir -p "$HOME/.virtualenvs"
        echo "export WORKON_HOME=$HOME/.virtualenvs" >> "$HOME/.bashrc"
        echo "source $(which virtualenvwrapper.sh)" >> "$HOME/.bashrc"
    fi
    export WORKON_HOME=$HOME/.virtualenvs
    # shellcheck source=/dev/null
    source "$(which virtualenvwrapper.sh)"
    # sphinxdoc-install-virtualenvwrapper-end

    # enable quitting on errors back:
    set -o errexit
    set -o nounset

}

setup_nginx_ubuntu14 () {
    # sphinxdoc-install-web-nginx-ubuntu14-begin
    # install Nginx web server:
    $sudo apt-get install -y nginx

    # configure Nginx web server:
    $sudo cp -f "$scriptpathname/../nginx/weko.conf" /etc/nginx/sites-available/
    $sudo sed -i "s,/home/invenio/,/home/$(whoami)/,g" /etc/nginx/sites-available/weko.conf
    $sudo rm /etc/nginx/sites-enabled/default
    $sudo ln -s /etc/nginx/sites-available/weko.conf /etc/nginx/sites-enabled/
    $sudo /usr/sbin/service nginx restart
    # sphinxdoc-install-web-nginx-ubuntu14-end
}

setup_nginx_centos7 () {
    # sphinxdoc-install-web-nginx-centos7-begin
    # install Nginx web server:
    $sudo yum install -y nginx

    # configure Nginx web server:
    $sudo cp "$scriptpathname/../nginx/weko.conf" /etc/nginx/conf.d/
    $sudo sed -i "s,/home/invenio/,/home/$(whoami)/,g" /etc/nginx/conf.d/weko.conf

    # add SELinux permissions if necessary:
    if $sudo getenforce | grep -q 'Enforcing'; then
        if ! $sudo semanage port -l | tail -n +1 | grep -q '8888'; then
            $sudo semanage port -a -t http_port_t -p tcp 8888
        fi
        if ! $sudo semanage port -l | grep ^http_port_t | grep -q 5000; then
            $sudo semanage port -m -t http_port_t -p tcp 5000
        fi
        if ! $sudo getsebool -a | grep httpd | grep httpd_enable_homedirs | grep -q on; then
            $sudo setsebool -P httpd_enable_homedirs 1
            # add static dir
            mkdir -p "/home/$(whoami)/.virtualenvs/${INVENIO_WEB_VENV}/var/instance/static"
            $sudo chcon -R -t httpd_sys_content_t "/home/$(whoami)/.virtualenvs/${INVENIO_WEB_VENV}/var/instance/static"
            # add data dir
            mkdir -p "/home/$(whoami)/.virtualenvs/${INVENIO_WEB_VENV}/var/instance/data"
            $sudo chcon -R -t httpd_sys_content_t "/home/$(whoami)/.virtualenvs/${INVENIO_WEB_VENV}/var/instance/data"
            # add conf dir
            mkdir -p "/home/$(whoami)/.virtualenvs/${INVENIO_WEB_VENV}/var/instance/conf"
            $sudo chcon -R -t httpd_sys_content_t "/home/$(whoami)/.virtualenvs/${INVENIO_WEB_VENV}/var/instance/conf"
        fi
    fi

    $sudo sed -i 's,80,8888,g' /etc/nginx/nginx.conf
    $sudo chmod go+rx "/home/$(whoami)/"
    $sudo /sbin/service nginx restart

    # open firewall ports:
    if firewall-cmd --state | grep -q running; then
        $sudo firewall-cmd --permanent --zone=public --add-service=http
        $sudo firewall-cmd --permanent --zone=public --add-service=https
        $sudo firewall-cmd --reload
    fi
    # sphinxdoc-install-web-nginx-centos7-end
}

setup_libreoffice_centos7 () {
    # sphinxdoc-install-web-libreoffice-centos7-begin
    set +o errexit
    $sudo yum install -y libreoffice
    $sudo yum install -y fonts-ipafont fonts-ipaexfont # japanese fonts
    set -o errexit
    # sphinxdoc-install-web-libreoffice-centos7-end
}


cleanup_web_centos7 () {
    # sphinxdoc-install-web-cleanup-centos7-begin
    $sudo yum clean -y all
    # sphinxdoc-install-web-cleanup-centos7-end
}

# Debian 系（docker / Ubuntu / Debian / WSL2）共通: 推奨パッケージを入れない最小構成
# 引数: docker | host
provision_web_debian_family () {
    local target="$1"
    local apt_install="$sudo apt-get -y install --no-install-recommends"

    $sudo apt-get -y update --allow-releaseinfo-change
    $apt_install \
         ca-certificates curl git rlwrap screen vim gnupg \
         libpcre2-dev

    if [[ ! -f /etc/apt/sources.list.d/nodesource.list ]]; then
        curl -sL https://deb.nodesource.com/setup_20.x | $sudo bash -
    fi
    # リダイレクトも sudo で行い、毎回同じ内容で上書きする
    printf "Package: *\nPin: origin deb.nodesource.com\nPin-Priority: 600\n" \
        | $sudo tee /etc/apt/preferences.d/nodesource >/dev/null
    $apt_install \
         gcc libc6-dev \
         libssl-dev libxml2-dev \
         libpq-dev \
         nodejs
    # pip と Python のヘッダが無いとき（ホストのシステムの Python）だけ入れる。
    # setup_virtualenvwrapper はこの pip を使い、psycopg2 と uwsgi はこのヘッダでビルドされる。
    # docker のベースイメージ（python:3.12）には /usr/local に両方あるので、分岐に関係なく入らない
    if ! command -v pip >/dev/null 2>&1; then
        $apt_install python3-pip
    fi
    if ! python3 -c 'import os, sysconfig; exit(not os.path.exists(os.path.join(sysconfig.get_paths()["include"], "Python.h")))' 2>/dev/null; then
        $apt_install python-dev-is-python3
    fi
    $sudo mkdir -p /usr/share/man/man1
    $apt_install \
         default-jre-headless \
         libreoffice-writer libreoffice-calc libreoffice-impress libreoffice-math \
         fonts-ipafont fonts-ipaexfont \
         fonts-dejavu-core fonts-liberation2 \
         fonts-crosextra-carlito fonts-crosextra-caladea \
         fonts-noto-core \
         supervisor
    # 必須コマンドの存在確認（set +o errexit で失敗が隠れないように）
    for c in libreoffice java gcc; do
        command -v "$c" >/dev/null || { echo "[ERROR] $c not found"; exit 1; }
    done
}

cleanup_web_debian_family () {
    local target="$1"
    $sudo apt-get -y autoremove && $sudo apt-get -y clean
    if [ "$target" = "docker" ]; then
        $sudo rm -rf /var/lib/apt/lists/*
    fi
}

main () {

    # detect OS distribution and release version:
    if hash lsb_release 2> /dev/null; then
        os_distribution=$(lsb_release -i | cut -f 2)
        os_release=$(lsb_release -r | cut -f 2 | grep -oE '[0-9]+\.' | cut -d. -f1 | head -1)
    elif [ -e /etc/redhat-release ]; then
        os_distribution=$(cut -d ' ' -f 1 /etc/redhat-release)
        os_release=$(grep -oE '[0-9]+\.' /etc/redhat-release | cut -d. -f1 | head -1)
    elif [ -e /etc/lsb-release ]; then
        os_distribution=$(grep DISTRIB_ID /etc/lsb-release | cut -d"=" -f 2)
        os_release=$(grep DISTRIB_RELEASE /etc/lsb-release | cut -d"=" -f 2)
    elif [ -e /etc/debian_version ]; then
        os_distribution="DEBIAN"
        os_release=$(cat /etc/debian_version)
    else
        os_distribution="UNDETECTED"
        os_release="UNDETECTED"
    fi
    echo "[INFO] provision-web: os=${os_distribution} rel=${os_release} WEKO_DOCKER_BUILD=${WEKO_DOCKER_BUILD:-unset}"
    # call appropriate provisioning functions:
    if [ "${WEKO_DOCKER_BUILD:-0}" = "1" ] || [ -f /.dockerinit ] || [ -f /.dockerenv ]; then
        echo "[INFO] provision-web: branch=docker"
        # running inside Docker
        provision_web_debian_family docker
        setup_npm_and_css_js_filters
        setup_virtualenvwrapper
        cleanup_web_debian_family docker
elif [ "$os_distribution" = "Ubuntu" ] || [ "$os_distribution" = "Debian" ] || [ "$os_distribution" = "DEBIAN" ]; then
        # Debian: lsb_release があると "Debian"、無いと "DEBIAN"（/etc/debian_version）になる。WSL2 も含む
        echo "[INFO] provision-web: branch=debian_family os=${os_distribution}"
        provision_web_debian_family host
        setup_npm_and_css_js_filters
        setup_virtualenvwrapper
        #setup_nginx_ubuntu14
        cleanup_web_debian_family host
    elif [ "$os_distribution" = "CentOS" ]; then
        # CentOS は非対応。CentOS 7 は EOL（2024-06-30）で、標準の Python が 2.7（python3 でも 3.6）のため、
        # Python 3.12 前提の WEKO は構築できない。docker（Dockerfile）で構築すること。
        # 以前の処理は参考として残す（呼び出しはしない）。
        echo "[ERROR] Sorry, CentOS ${os_release} is not supported. Please use Docker."
        exit 1
        #echo "[INFO] provision-web: branch=centos"
        #if [ "$os_release" = "7" ]; then
        #    provision_web_common_centos7
        #    provision_web_libpostgresql_centos7
        #    setup_npm_and_css_js_filters
        #    setup_virtualenvwrapper
        #    #setup_nginx_centos7
        #    setup_libreoffice_centos7
        #    cleanup_web_centos7
        #else
        #    echo "[ERROR] Sorry, unsupported release ${os_release}."
        #    exit 1
        #fi
    else
        echo "[ERROR] Sorry, unsupported distribution ${os_distribution}."
        exit 1
    fi

}

main
