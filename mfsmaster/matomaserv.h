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

#endif
