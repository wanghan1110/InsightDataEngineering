# kill the process
# tmux kill-session -t k1
# tmux kill-session -t k2
# tmux kill-session -t k3
# tmux kill-session -t k4
ps -efw | grep 'python kafka_producer.py' | grep -v grep | awk '{print $2}' | xargs kill