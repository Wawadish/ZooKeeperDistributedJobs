#!/bin/bash 

#TODO: SPECIFY THE HOSTNAMES OF 4 CS MACHINES (lab2-8, lab2-10, etc...)
MACHINES=(lab2-10 lab2-11 lab2-13)

tmux new-session \; \
	split-window -v \; \
	split-window -v \; \
	select-layout main-vertical \; \
	select-pane -t 0 \; \
	send-keys "ssh -t ${MACHINES[0]} \"cd $(pwd)/${MACHINES[0]}; echo -n 'Connected to '; hostname; ../bin/zkServer.sh start zoo-base.cfg\"" C-m \; \
	select-pane -t 1 \; \
	send-keys "ssh -t ${MACHINES[1]} \"cd $(pwd)/${MACHINES[1]}; echo -n 'Connected to '; hostname; ../bin/zkServer.sh start zoo-base.cfg\"" C-m \; \
	select-pane -t 2 \; \
	send-keys "ssh -t ${MACHINES[2]} \"cd $(pwd)/${MACHINES[2]}; echo -n 'Connected to '; hostname; ../bin/zkServer.sh start zoo-base.cfg\"" C-m \; \