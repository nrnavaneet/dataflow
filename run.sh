#!/bin/bash

# Run producer.py in first terminal
osascript <<EOF
tell application "Terminal"
    do script "cd $(pwd) && source venv/bin/activate && python3 scripts/producer.py"
end tell
EOF


# Run producer.py in second terminal
osascript <<EOF
tell application "Terminal"
    do script "cd $(pwd) && source venv/bin/activate && python3 scripts/producer.py"
end tell
EOF

# Wait 3 seconds before starting consumer
sleep 3

# Run consumer.py in third terminal
osascript <<EOF
tell application "Terminal"
    do script "cd $(pwd) && source venv/bin/activate && python scripts/consumer.py"
end tell
EOF
