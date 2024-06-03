# Update package lists
sudo apt update
echo "Updating package lists..."

# Install dependencies for building Python from source
sudo apt install build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev libsqlite3-dev wget libbz2-dev -y
sudo apt-get install libncursesw5-dev tk-dev libc6-dev -y
echo "Installing dependencies for building Python..."

# Download Python source code
wget https://www.python.org/ftp/python/3.10.0/Python-3.10.0.tgz
echo "Downloading Python 3.10.0 source code..."

# Extract the downloaded archive
tar -xvf Python-3.10.0.tgz
echo "Extracting Python source code..."

# Enter the extracted directory
cd Python-3.10.0
echo "Changing directory to Python source..."

# Configure Python build with optimizations
sudo ./configure --enable-optimizations
echo "Configuring Python build with optimizations..."

# Build Python using 2 cores (adjust as needed)
sudo make -j 2
echo "Building Python (using 2 cores)..."

# Check the number of available cores (informational)
nproc
echo "Number of available cores (for reference)..."

# Install Python locally (without overwriting system Python)
sudo make altinstall
echo "Installing Python locally..."

# Verify Python installation
python3.10 --version
echo "Verifying Python installation..."

# Install pip using downloaded script
curl -sS https://bootstrap.pypa.io/get-pip.py | python3.10
echo "Installing pip..."

# Set up alternatives for python and pip (pointing to local installation)
update-alternatives --install /usr/bin/python python /usr/local/bin/python3.10 1
update-alternatives --install /usr/bin/pip pip /usr/local/bin/pip3.10 1
echo "Setting up alternatives for python and pip..."

# Install poetry using pip
pip install poetry
echo "Installing poetry..."