#!/bin/bash
# EC2 setup script — run as ubuntu user after SSH'ing in

# System packages
sudo apt update && sudo apt install -y python3-pip nginx git

# Node 20
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs

# Python dependencies
pip3 install --break-system-packages numpy websockets

# Clone repo
cd ~
git clone https://github.com/YOUR_USERNAME/Nasdaq-ITCH-market-simulator.git
cd Nasdaq-ITCH-market-simulator

# Create data directory and upload ITCH file
mkdir -p data
echo "Upload your ITCH file now:"
echo "  scp -i your-key.pem data/01302019.NASDAQ_ITCH50 ubuntu@YOUR_IP:~/Nasdaq-ITCH-market-simulator/data/"
echo "Press Enter when done..."
read

# Build React frontend
cd market-ui
npm install
npm run build
cd ..

# Nginx config
sudo tee /etc/nginx/sites-available/market-ui > /dev/null <<'EOF'
server {
    listen 80;

    root /home/ubuntu/Nasdaq-ITCH-market-simulator/market-ui/dist;
    index index.html;

    location / {
        try_files $uri $uri/ /index.html;
    }

    location /ws {
        proxy_pass http://127.0.0.1:8765;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 86400;
    }
}
EOF

sudo ln -s /etc/nginx/sites-available/market-ui /etc/nginx/sites-enabled/ 2>/dev/null
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t && sudo systemctl restart nginx

# Permissions
chmod 755 /home/ubuntu
chmod -R 755 /home/ubuntu/Nasdaq-ITCH-market-simulator/market-ui/dist

# Multicast setup
sudo ip link set lo multicast on
sudo ip route add 224.0.0.0/4 dev lo 2>/dev/null

# Make multicast persist across reboots
sudo tee /etc/rc.local > /dev/null <<'EOF'
#!/bin/bash
ip link set lo multicast on
ip route add 224.0.0.0/4 dev lo
exit 0
EOF
sudo chmod +x /etc/rc.local

# Increase UDP buffer
sudo sysctl -w net.core.rmem_max=16777216
sudo sysctl -w net.core.rmem_default=8388608
echo "net.core.rmem_max=16777216" | sudo tee -a /etc/sysctl.conf
echo "net.core.rmem_default=8388608" | sudo tee -a /etc/sysctl.conf

echo ""
echo "Setup complete. To start:"
echo "  cd ~/Nasdaq-ITCH-market-simulator"
echo "  nohup python3 orchestrator.py > orchestrator.log 2>&1 &"
echo ""
echo "To monitor:"
echo "  tail -f orchestrator.log"