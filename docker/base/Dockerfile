FROM centos:6

RUN yum -y update \
    && yum -y groupinstall "Development tools" \
    && yum -y install apr-devel         \
                      bzip2-devel       \
                      ccache            \
                      curl-devel        \
                      ed                \
                      epel-release      \
                      htop              \
                      iproute           \
                      libevent-devel    \
                      libffi-devel      \
                      libxml2-devel     \
                      libyaml           \
                      libyaml-devel     \
                      mc                \
                      net-tools         \
                      openssl-devel     \
                      openssl-libs      \
                      openssh-server    \
                      perl-CPAN         \
                      perl-devel        \
                      perl-ExtUtils-Embed   \
                      psmisc            \
                      python-devel      \
                      readline-devel    \
                      vim               \
                      zlib-devel

RUN curl -SL https://bootstrap.pypa.io/get-pip.py | python \
    && pip install psutil lockfile paramiko setuptools epydoc

# cmake 3.0
WORKDIR /root

ADD http://www.cmake.org/files/v3.0/cmake-3.0.0.tar.gz ./
RUN tar -zxvf cmake-3.0.0.tar.gz
WORKDIR cmake-3.0.0
RUN ./bootstrap && make && make install
ENV PATH=/usr/local/bin:$PATH

# compile GPDB

WORKDIR /workspace

ADD . gpdb/
ADD ./docker/dotfiles/root.bashrc /root/.bashrc
ADD ./docker/dotfiles/gpadmin.bashrc /home/gpadmin/.bashrc

WORKDIR gpdb

RUN ./configure --with-python --with-perl --enable-mapreduce \
                --with-libxml --prefix=/usr/local/gpdb \
    && time make -j4    \
    && make install     \
    && ./docker/prep_for_ssh.bash \
    && chown -R gpadmin:gpadmin /home/gpadmin \
    && chmod u+s `which ping`
