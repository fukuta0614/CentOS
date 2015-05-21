sudo yum install -y zlib-devel bzip2-devel openssl-devel sqlite-devel kernel-devel-$(uname -r) ncurses-devel readline
wget https://www.python.org/ftp/python/3.4.0/Python-3.4.0.tar.xz
tar -xvf Python-3.4.0.tar.xz
cd Python-3.4.0
./configure --enable-shared --prefix=/usr/local LDFLAGS="-Wl,-rpath /usr/local/lib"
make
sudo make install
