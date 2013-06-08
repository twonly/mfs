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

#ifndef _matomaSERV_H_
#define _matomaSERV_H_

#include <inttypes.h>

uint32_t matomaserv_mloglist_size(void);
void matomaserv_mloglist_data(uint8_t *ptr);

void matomaserv_broadcast_logstring(uint64_t version,uint8_t *logstr,uint32_t logstrsize);
void matomaserv_broadcast_logrotate();
int matomaserv_init(void);

typedef struct packetstruct {
	struct packetstruct *next;
	uint8_t *startptr;
	uint32_t bytesleft;
	uint8_t *packet;
} packetstruct;

typedef struct matomaserventry {
	uint8_t mode;
	int sock;
	int32_t pdescpos;
	uint32_t lastread,lastwrite;
	uint8_t hdrbuff[8];
	packetstruct inputpacket;
	packetstruct *outputhead,**outputtail;

	uint16_t timeout;

	char *servstrip;		// human readable version of servip
	uint32_t version;
	uint32_t servip;

	int metafd,chain1fd,chain2fd; //what does 1fd and 2fd mean?

	struct matomaserventry *next;
} matomaserventry;

extern matomaserventry *matomaservhead;//=NULL;
//static matomaserventry *matomaservhead=NULL;

#endif
