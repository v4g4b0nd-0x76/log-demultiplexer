# LOG DEMULTIPLEXER

this project is sample and R&D project with a logic similar to vectordev wich accept one log as udp conneciton and demultiplex it into many resources for now i consider implementing elastic search and redis

the process shal be dynamically configurable such as buffer size, the pre allocated memory size, destination channles(elastic and redis) and their limitation, backpressure and drop policy and whether the logs shall be persistent or not

the input can be json or string

**THIS PROJECT PROBABLY WONT SUIT PRODUCTION AND DOES NOT WORTH CONTRIBUTION AFFORT DU TO 'vectordev' EXISTENCE**
