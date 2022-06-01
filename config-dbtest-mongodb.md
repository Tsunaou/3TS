# Configuration Coo for MongoDB

## 1. 安装 mongocxx driver
> 官方文档： http://mongocxx.org/mongocxx-v3/installation/linux/


### 1.1 安装 mongoc driver
> 官方文档： http://mongoc.org/libmongoc/current/installing.html

MongoDB C++驱动依赖于MongoDB C驱动，我们需要先安装C驱动。尽管yum上有源，但是yum上的源也是编译好的，因此我们还是得自己安装。

首先安装openSSL与Cyrus SASL
```bash
yum install cmake openssl-devel cyrus-sasl-devel
```
然后下载release包编译构建
```bash
wget https://github.com/mongodb/mongo-c-driver/releases/download/1.21.1/mongo-c-driver-1.21.1.tar.gz
tar xzf mongo-c-driver-1.21.1.tar.gz
cd mongo-c-driver-1.21.1
mkdir cmake-build
cd cmake-build
cmake -DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF ..
```
最后看到输出
```bash
-- Build files have been written to: /root/programs/mongo-c-driver-1.21.1/cmake-build
```
然后输入命令
```bash
cmake --build .
sudo cmake --build . --target install
```
安装后的文件基本分布在
- `/usr/local/share`
- `/usr/local/lib64/`
- `/usr/local/include`


### 1.2 选择 C++17 polyfill
MongoDB的C++ driver依赖于C++ 17的特性，如果默认的gcc版本太老，需要手动升级。我使用的是基于CentOS-7的系统，需要自己更新
```
scl enable devtoolset-11 zsh # method1 
source /opt/rh/devtoolset-11/enable # method2

```

### 1.3 下载 mongocxx driver
```bash
curl -OL https://github.com/mongodb/mongo-cxx-driver/releases/download/r3.6.7/mongo-cxx-driver-r3.6.7.tar.gz
tar -xzf mongo-cxx-driver-r3.6.7.tar.gz
cd mongo-cxx-driver-r3.6.7/build
```

### 1.4 配置 CMake与driver
> 官方文档： https://github.com/Kitware/CMake

驱动的编译与构建需要大于等于CMake 3.2的cmake，同样是需要自己构建

```bash
https://github.com/Kitware/CMake.git
./bootstrap && make && sudo make install
```

在Unix的系统里，`libmongoc`默认安装在`/usr/local`，我们也将`mongocxx`安装在默认位置。在`build`目录下，
```bash
cmake ..                                \
    -DCMAKE_BUILD_TYPE=Release          \
    -DCMAKE_INSTALL_PREFIX=/usr/local
```
### 1.5 构建并安装mongocxx dirver
```
cmake --build .
sudo cmake --build . --target install
```
同样的，安装后的文件基本分布在
- `/usr/local/share`
- `/usr/local/lib64/`
- `/usr/local/include`

## 2. 安装其他依赖
### 2.1 安装gflags
> 官方文档：https://github.com/gflags/gflags/blob/master/INSTALL.md
```bash
git clone https://github.com/gflags/gflags.git
cd gflags
mkdir build && cd build
ccmake ..
  - Press 'c' to configure the build system and 'e' to ignore warnings.
  - Set CMAKE_INSTALL_PREFIX and other CMake variables and options.
  - Continue pressing 'c' until the option 'g' is available.
  - Then press 'g' to generate the configuration files for GNU Make.
make
make install
```