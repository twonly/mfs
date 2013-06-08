/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA.

   This file is part of MooseFS.

   MooseFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   MooseFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with MooseFS.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef _MASTERCONN_H_
#define _MASTERCONN_H_

#include <inttypes.h>
#include "matomaserv.h"

//void masterconn_stats(uint64_t *bin,uint64_t *bout,uint32_t *maxjobscnt);
// void masterconn_replicate_status(uint64_t chunkid,uint32_t version,uint8_t status);
// void masterconn_send_chunk_damaged(uint64_t chunkid);
// void masterconn_send_chunk_lost(uint64_t chunkid);
// void masterconn_send_error_occurred();
// void masterconn_send_space(uint64_t usedspace,uint64_t totalspace,uint32_t chunkcount,uint64_t tdusedspace,uint64_t tdtotalspace,uint32_t tdchunkcount);
int masterconn_init(void);

/*typedef struct packetstruct {
	struct packetstruct *next;
	uint8_t *startptr;
	uint32_t bytesleft;
	uint8_t *packet;
} packetstruct; */

typedef struct masterconn {
	int mode;
	int sock;
	int32_t pdescpos;
	uint32_t lastread,lastwrite;
	uint8_t hdrbuff[8];
	packetstruct inputpacket;
	packetstruct *outputhead,**outputtail;
	uint32_t bindip;
	uint32_t masterip;
	uint16_t masterport;
	uint8_t masteraddrvalid;
} masterconn;

extern masterconn *masterconnsingleton; //yujy =NULL; //singleton means a metalogger can only connect to one MASTER

#endif
