#define rand	pan_rand
#define pthread_equal(a,b)	((a)==(b))
#if defined(HAS_CODE) && defined(VERBOSE)
	#ifdef BFS_PAR
		bfs_printf("Pr: %d Tr: %d\n", II, t->forw);
	#else
		cpu_printf("Pr: %d Tr: %d\n", II, t->forw);
	#endif
#endif
	switch (t->forw) {
	default: Uerror("bad forward move");
	case 0:	/* if without executable clauses */
		continue;
	case 1: /* generic 'goto' or 'skip' */
		IfNotBlocked
		_m = 3; goto P999;
	case 2: /* generic 'else' */
		IfNotBlocked
		if (trpt->o_pm&1) continue;
		_m = 3; goto P999;

		 /* CLAIM buffer_seguro */
	case 3: // STATE 1 - _spin_nvr.tmp:56 - [(!(((buffer_count>=0)&&(buffer_count<=5))))] (6:0:0 - 1)
		
#if defined(VERI) && !defined(NP)
#if NCLAIMS>1
		{	static int reported1 = 0;
			if (verbose && !reported1)
			{	int nn = (int) ((Pclaim *)pptr(0))->_n;
				printf("depth %ld: Claim %s (%d), state %d (line %d)\n",
					depth, procname[spin_c_typ[nn]], nn, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported1 = 1;
				fflush(stdout);
		}	}
#else
		{	static int reported1 = 0;
			if (verbose && !reported1)
			{	printf("depth %d: Claim, state %d (line %d)\n",
					(int) depth, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported1 = 1;
				fflush(stdout);
		}	}
#endif
#endif
		reached[7][1] = 1;
		if (!( !(((((int)now.buffer_count)>=0)&&(((int)now.buffer_count)<=5)))))
			continue;
		/* merge: assert(!(!(((buffer_count>=0)&&(buffer_count<=5)))))(0, 2, 6) */
		reached[7][2] = 1;
		spin_assert( !( !(((((int)now.buffer_count)>=0)&&(((int)now.buffer_count)<=5)))), " !( !(((buffer_count>=0)&&(buffer_count<=5))))", II, tt, t);
		/* merge: .(goto)(0, 7, 6) */
		reached[7][7] = 1;
		;
		_m = 3; goto P999; /* 2 */
	case 4: // STATE 10 - _spin_nvr.tmp:61 - [-end-] (0:0:0 - 1)
		
#if defined(VERI) && !defined(NP)
#if NCLAIMS>1
		{	static int reported10 = 0;
			if (verbose && !reported10)
			{	int nn = (int) ((Pclaim *)pptr(0))->_n;
				printf("depth %ld: Claim %s (%d), state %d (line %d)\n",
					depth, procname[spin_c_typ[nn]], nn, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported10 = 1;
				fflush(stdout);
		}	}
#else
		{	static int reported10 = 0;
			if (verbose && !reported10)
			{	printf("depth %d: Claim, state %d (line %d)\n",
					(int) depth, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported10 = 1;
				fflush(stdout);
		}	}
#endif
#endif
		reached[7][10] = 1;
		if (!delproc(1, II)) continue;
		_m = 3; goto P999; /* 0 */

		 /* CLAIM anti_starvation_worker2 */
	case 5: // STATE 1 - _spin_nvr.tmp:45 - [((!(!(worker_active[2]))&&!((worker_jobs[2]>0))))] (0:0:0 - 1)
		
#if defined(VERI) && !defined(NP)
#if NCLAIMS>1
		{	static int reported1 = 0;
			if (verbose && !reported1)
			{	int nn = (int) ((Pclaim *)pptr(0))->_n;
				printf("depth %ld: Claim %s (%d), state %d (line %d)\n",
					depth, procname[spin_c_typ[nn]], nn, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported1 = 1;
				fflush(stdout);
		}	}
#else
		{	static int reported1 = 0;
			if (verbose && !reported1)
			{	printf("depth %d: Claim, state %d (line %d)\n",
					(int) depth, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported1 = 1;
				fflush(stdout);
		}	}
#endif
#endif
		reached[6][1] = 1;
		if (!(( !( !(((int)now.worker_active[2])))&& !((((int)now.worker_jobs[2])>0)))))
			continue;
		_m = 3; goto P999; /* 0 */
	case 6: // STATE 8 - _spin_nvr.tmp:50 - [(!((worker_jobs[2]>0)))] (0:0:0 - 1)
		
#if defined(VERI) && !defined(NP)
#if NCLAIMS>1
		{	static int reported8 = 0;
			if (verbose && !reported8)
			{	int nn = (int) ((Pclaim *)pptr(0))->_n;
				printf("depth %ld: Claim %s (%d), state %d (line %d)\n",
					depth, procname[spin_c_typ[nn]], nn, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported8 = 1;
				fflush(stdout);
		}	}
#else
		{	static int reported8 = 0;
			if (verbose && !reported8)
			{	printf("depth %d: Claim, state %d (line %d)\n",
					(int) depth, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported8 = 1;
				fflush(stdout);
		}	}
#endif
#endif
		reached[6][8] = 1;
		if (!( !((((int)now.worker_jobs[2])>0))))
			continue;
		_m = 3; goto P999; /* 0 */
	case 7: // STATE 13 - _spin_nvr.tmp:52 - [-end-] (0:0:0 - 1)
		
#if defined(VERI) && !defined(NP)
#if NCLAIMS>1
		{	static int reported13 = 0;
			if (verbose && !reported13)
			{	int nn = (int) ((Pclaim *)pptr(0))->_n;
				printf("depth %ld: Claim %s (%d), state %d (line %d)\n",
					depth, procname[spin_c_typ[nn]], nn, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported13 = 1;
				fflush(stdout);
		}	}
#else
		{	static int reported13 = 0;
			if (verbose && !reported13)
			{	printf("depth %d: Claim, state %d (line %d)\n",
					(int) depth, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported13 = 1;
				fflush(stdout);
		}	}
#endif
#endif
		reached[6][13] = 1;
		if (!delproc(1, II)) continue;
		_m = 3; goto P999; /* 0 */

		 /* CLAIM anti_starvation_worker1 */
	case 8: // STATE 1 - _spin_nvr.tmp:34 - [((!(!(worker_active[1]))&&!((worker_jobs[1]>0))))] (0:0:0 - 1)
		
#if defined(VERI) && !defined(NP)
#if NCLAIMS>1
		{	static int reported1 = 0;
			if (verbose && !reported1)
			{	int nn = (int) ((Pclaim *)pptr(0))->_n;
				printf("depth %ld: Claim %s (%d), state %d (line %d)\n",
					depth, procname[spin_c_typ[nn]], nn, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported1 = 1;
				fflush(stdout);
		}	}
#else
		{	static int reported1 = 0;
			if (verbose && !reported1)
			{	printf("depth %d: Claim, state %d (line %d)\n",
					(int) depth, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported1 = 1;
				fflush(stdout);
		}	}
#endif
#endif
		reached[5][1] = 1;
		if (!(( !( !(((int)now.worker_active[1])))&& !((((int)now.worker_jobs[1])>0)))))
			continue;
		_m = 3; goto P999; /* 0 */
	case 9: // STATE 8 - _spin_nvr.tmp:39 - [(!((worker_jobs[1]>0)))] (0:0:0 - 1)
		
#if defined(VERI) && !defined(NP)
#if NCLAIMS>1
		{	static int reported8 = 0;
			if (verbose && !reported8)
			{	int nn = (int) ((Pclaim *)pptr(0))->_n;
				printf("depth %ld: Claim %s (%d), state %d (line %d)\n",
					depth, procname[spin_c_typ[nn]], nn, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported8 = 1;
				fflush(stdout);
		}	}
#else
		{	static int reported8 = 0;
			if (verbose && !reported8)
			{	printf("depth %d: Claim, state %d (line %d)\n",
					(int) depth, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported8 = 1;
				fflush(stdout);
		}	}
#endif
#endif
		reached[5][8] = 1;
		if (!( !((((int)now.worker_jobs[1])>0))))
			continue;
		_m = 3; goto P999; /* 0 */
	case 10: // STATE 13 - _spin_nvr.tmp:41 - [-end-] (0:0:0 - 1)
		
#if defined(VERI) && !defined(NP)
#if NCLAIMS>1
		{	static int reported13 = 0;
			if (verbose && !reported13)
			{	int nn = (int) ((Pclaim *)pptr(0))->_n;
				printf("depth %ld: Claim %s (%d), state %d (line %d)\n",
					depth, procname[spin_c_typ[nn]], nn, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported13 = 1;
				fflush(stdout);
		}	}
#else
		{	static int reported13 = 0;
			if (verbose && !reported13)
			{	printf("depth %d: Claim, state %d (line %d)\n",
					(int) depth, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported13 = 1;
				fflush(stdout);
		}	}
#endif
#endif
		reached[5][13] = 1;
		if (!delproc(1, II)) continue;
		_m = 3; goto P999; /* 0 */

		 /* CLAIM anti_starvation_worker0 */
	case 11: // STATE 1 - _spin_nvr.tmp:23 - [((!(!(worker_active[0]))&&!((worker_jobs[0]>0))))] (0:0:0 - 1)
		
#if defined(VERI) && !defined(NP)
#if NCLAIMS>1
		{	static int reported1 = 0;
			if (verbose && !reported1)
			{	int nn = (int) ((Pclaim *)pptr(0))->_n;
				printf("depth %ld: Claim %s (%d), state %d (line %d)\n",
					depth, procname[spin_c_typ[nn]], nn, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported1 = 1;
				fflush(stdout);
		}	}
#else
		{	static int reported1 = 0;
			if (verbose && !reported1)
			{	printf("depth %d: Claim, state %d (line %d)\n",
					(int) depth, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported1 = 1;
				fflush(stdout);
		}	}
#endif
#endif
		reached[4][1] = 1;
		if (!(( !( !(((int)now.worker_active[0])))&& !((((int)now.worker_jobs[0])>0)))))
			continue;
		_m = 3; goto P999; /* 0 */
	case 12: // STATE 8 - _spin_nvr.tmp:28 - [(!((worker_jobs[0]>0)))] (0:0:0 - 1)
		
#if defined(VERI) && !defined(NP)
#if NCLAIMS>1
		{	static int reported8 = 0;
			if (verbose && !reported8)
			{	int nn = (int) ((Pclaim *)pptr(0))->_n;
				printf("depth %ld: Claim %s (%d), state %d (line %d)\n",
					depth, procname[spin_c_typ[nn]], nn, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported8 = 1;
				fflush(stdout);
		}	}
#else
		{	static int reported8 = 0;
			if (verbose && !reported8)
			{	printf("depth %d: Claim, state %d (line %d)\n",
					(int) depth, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported8 = 1;
				fflush(stdout);
		}	}
#endif
#endif
		reached[4][8] = 1;
		if (!( !((((int)now.worker_jobs[0])>0))))
			continue;
		_m = 3; goto P999; /* 0 */
	case 13: // STATE 13 - _spin_nvr.tmp:30 - [-end-] (0:0:0 - 1)
		
#if defined(VERI) && !defined(NP)
#if NCLAIMS>1
		{	static int reported13 = 0;
			if (verbose && !reported13)
			{	int nn = (int) ((Pclaim *)pptr(0))->_n;
				printf("depth %ld: Claim %s (%d), state %d (line %d)\n",
					depth, procname[spin_c_typ[nn]], nn, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported13 = 1;
				fflush(stdout);
		}	}
#else
		{	static int reported13 = 0;
			if (verbose && !reported13)
			{	printf("depth %d: Claim, state %d (line %d)\n",
					(int) depth, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported13 = 1;
				fflush(stdout);
		}	}
#endif
#endif
		reached[4][13] = 1;
		if (!delproc(1, II)) continue;
		_m = 3; goto P999; /* 0 */

		 /* CLAIM progreso */
	case 14: // STATE 1 - _spin_nvr.tmp:12 - [((!(!((produced>consumed)))&&!((consumed==produced))))] (0:0:0 - 1)
		
#if defined(VERI) && !defined(NP)
#if NCLAIMS>1
		{	static int reported1 = 0;
			if (verbose && !reported1)
			{	int nn = (int) ((Pclaim *)pptr(0))->_n;
				printf("depth %ld: Claim %s (%d), state %d (line %d)\n",
					depth, procname[spin_c_typ[nn]], nn, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported1 = 1;
				fflush(stdout);
		}	}
#else
		{	static int reported1 = 0;
			if (verbose && !reported1)
			{	printf("depth %d: Claim, state %d (line %d)\n",
					(int) depth, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported1 = 1;
				fflush(stdout);
		}	}
#endif
#endif
		reached[3][1] = 1;
		if (!(( !( !((((int)now.produced)>((int)now.consumed))))&& !((((int)now.consumed)==((int)now.produced))))))
			continue;
		_m = 3; goto P999; /* 0 */
	case 15: // STATE 8 - _spin_nvr.tmp:17 - [(!((consumed==produced)))] (0:0:0 - 1)
		
#if defined(VERI) && !defined(NP)
#if NCLAIMS>1
		{	static int reported8 = 0;
			if (verbose && !reported8)
			{	int nn = (int) ((Pclaim *)pptr(0))->_n;
				printf("depth %ld: Claim %s (%d), state %d (line %d)\n",
					depth, procname[spin_c_typ[nn]], nn, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported8 = 1;
				fflush(stdout);
		}	}
#else
		{	static int reported8 = 0;
			if (verbose && !reported8)
			{	printf("depth %d: Claim, state %d (line %d)\n",
					(int) depth, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported8 = 1;
				fflush(stdout);
		}	}
#endif
#endif
		reached[3][8] = 1;
		if (!( !((((int)now.consumed)==((int)now.produced)))))
			continue;
		_m = 3; goto P999; /* 0 */
	case 16: // STATE 13 - _spin_nvr.tmp:19 - [-end-] (0:0:0 - 1)
		
#if defined(VERI) && !defined(NP)
#if NCLAIMS>1
		{	static int reported13 = 0;
			if (verbose && !reported13)
			{	int nn = (int) ((Pclaim *)pptr(0))->_n;
				printf("depth %ld: Claim %s (%d), state %d (line %d)\n",
					depth, procname[spin_c_typ[nn]], nn, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported13 = 1;
				fflush(stdout);
		}	}
#else
		{	static int reported13 = 0;
			if (verbose && !reported13)
			{	printf("depth %d: Claim, state %d (line %d)\n",
					(int) depth, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported13 = 1;
				fflush(stdout);
		}	}
#endif
#endif
		reached[3][13] = 1;
		if (!delproc(1, II)) continue;
		_m = 3; goto P999; /* 0 */

		 /* CLAIM exclusion_mutua */
	case 17: // STATE 1 - _spin_nvr.tmp:3 - [(!(!((in_critical&&(mutex==0)))))] (6:0:0 - 1)
		
#if defined(VERI) && !defined(NP)
#if NCLAIMS>1
		{	static int reported1 = 0;
			if (verbose && !reported1)
			{	int nn = (int) ((Pclaim *)pptr(0))->_n;
				printf("depth %ld: Claim %s (%d), state %d (line %d)\n",
					depth, procname[spin_c_typ[nn]], nn, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported1 = 1;
				fflush(stdout);
		}	}
#else
		{	static int reported1 = 0;
			if (verbose && !reported1)
			{	printf("depth %d: Claim, state %d (line %d)\n",
					(int) depth, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported1 = 1;
				fflush(stdout);
		}	}
#endif
#endif
		reached[2][1] = 1;
		if (!( !( !((((int)now.in_critical)&&(((int)now.mutex)==0))))))
			continue;
		/* merge: assert(!(!(!((in_critical&&(mutex==0))))))(0, 2, 6) */
		reached[2][2] = 1;
		spin_assert( !( !( !((((int)now.in_critical)&&(((int)now.mutex)==0))))), " !( !( !((in_critical&&(mutex==0)))))", II, tt, t);
		/* merge: .(goto)(0, 7, 6) */
		reached[2][7] = 1;
		;
		_m = 3; goto P999; /* 2 */
	case 18: // STATE 10 - _spin_nvr.tmp:8 - [-end-] (0:0:0 - 1)
		
#if defined(VERI) && !defined(NP)
#if NCLAIMS>1
		{	static int reported10 = 0;
			if (verbose && !reported10)
			{	int nn = (int) ((Pclaim *)pptr(0))->_n;
				printf("depth %ld: Claim %s (%d), state %d (line %d)\n",
					depth, procname[spin_c_typ[nn]], nn, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported10 = 1;
				fflush(stdout);
		}	}
#else
		{	static int reported10 = 0;
			if (verbose && !reported10)
			{	printf("depth %d: Claim, state %d (line %d)\n",
					(int) depth, (int) ((Pclaim *)pptr(0))->_p, src_claim[ (int) ((Pclaim *)pptr(0))->_p ]);
				reported10 = 1;
				fflush(stdout);
		}	}
#endif
#endif
		reached[2][10] = 1;
		if (!delproc(1, II)) continue;
		_m = 3; goto P999; /* 0 */

		 /* PROC Limpiador */
	case 19: // STATE 1 - modelo_final.pml:87 - [id = (_pid-1)] (0:0:1 - 1)
		IfNotBlocked
		reached[1][1] = 1;
		(trpt+1)->bup.oval = ((int)((P1 *)_this)->id);
		((P1 *)_this)->id = (((int)((P1 *)_this)->_pid)-1);
#ifdef VAR_RANGES
		logval("Limpiador:id", ((int)((P1 *)_this)->id));
#endif
		;
		_m = 3; goto P999; /* 0 */
	case 20: // STATE 2 - modelo_final.pml:93 - [(((buffer_count>0)&&!(mutex)))] (17:0:8 - 1)
		IfNotBlocked
		reached[1][2] = 1;
		if (!(((((int)now.buffer_count)>0)&& !(((int)now.mutex)))))
			continue;
		/* merge: mutex = 1(17, 3, 17) */
		reached[1][3] = 1;
		(trpt+1)->bup.ovals = grab_ints(8);
		(trpt+1)->bup.ovals[0] = ((int)now.mutex);
		now.mutex = 1;
#ifdef VAR_RANGES
		logval("mutex", ((int)now.mutex));
#endif
		;
		/* merge: assert(!(in_critical))(17, 4, 17) */
		reached[1][4] = 1;
		spin_assert( !(((int)now.in_critical)), " !(in_critical)", II, tt, t);
		/* merge: in_critical = 1(17, 5, 17) */
		reached[1][5] = 1;
		(trpt+1)->bup.ovals[1] = ((int)now.in_critical);
		now.in_critical = 1;
#ifdef VAR_RANGES
		logval("in_critical", ((int)now.in_critical));
#endif
		;
		/* merge: buffer_count = (buffer_count-1)(17, 6, 17) */
		reached[1][6] = 1;
		(trpt+1)->bup.ovals[2] = ((int)now.buffer_count);
		now.buffer_count = (((int)now.buffer_count)-1);
#ifdef VAR_RANGES
		logval("buffer_count", ((int)now.buffer_count));
#endif
		;
		/* merge: consumed = (consumed+1)(17, 7, 17) */
		reached[1][7] = 1;
		(trpt+1)->bup.ovals[3] = ((int)now.consumed);
		now.consumed = (((int)now.consumed)+1);
#ifdef VAR_RANGES
		logval("consumed", ((int)now.consumed));
#endif
		;
		/* merge: worker_jobs[id] = (worker_jobs[id]+1)(17, 8, 17) */
		reached[1][8] = 1;
		(trpt+1)->bup.ovals[4] = ((int)now.worker_jobs[ Index(((int)((P1 *)_this)->id), 3) ]);
		now.worker_jobs[ Index(((P1 *)_this)->id, 3) ] = (((int)now.worker_jobs[ Index(((int)((P1 *)_this)->id), 3) ])+1);
#ifdef VAR_RANGES
		logval("worker_jobs[Limpiador:id]", ((int)now.worker_jobs[ Index(((int)((P1 *)_this)->id), 3) ]));
#endif
		;
		/* merge: worker_active[id] = 1(17, 9, 17) */
		reached[1][9] = 1;
		(trpt+1)->bup.ovals[5] = ((int)now.worker_active[ Index(((int)((P1 *)_this)->id), 3) ]);
		now.worker_active[ Index(((P1 *)_this)->id, 3) ] = 1;
#ifdef VAR_RANGES
		logval("worker_active[Limpiador:id]", ((int)now.worker_active[ Index(((int)((P1 *)_this)->id), 3) ]));
#endif
		;
		/* merge: printf('Worker %d: Procesando expediente. Buffer=%d\\n',_pid,buffer_count)(17, 10, 17) */
		reached[1][10] = 1;
		Printf("Worker %d: Procesando expediente. Buffer=%d\n", ((int)((P1 *)_this)->_pid), ((int)now.buffer_count));
		/* merge: in_critical = 0(17, 11, 17) */
		reached[1][11] = 1;
		(trpt+1)->bup.ovals[6] = ((int)now.in_critical);
		now.in_critical = 0;
#ifdef VAR_RANGES
		logval("in_critical", ((int)now.in_critical));
#endif
		;
		/* merge: mutex = 0(17, 12, 17) */
		reached[1][12] = 1;
		(trpt+1)->bup.ovals[7] = ((int)now.mutex);
		now.mutex = 0;
#ifdef VAR_RANGES
		logval("mutex", ((int)now.mutex));
#endif
		;
		/* merge: printf('Worker %d: Aplicando limpieza y anonimización...\\n',_pid)(17, 14, 17) */
		reached[1][14] = 1;
		Printf("Worker %d: Aplicando limpieza y anonimización...\n", ((int)((P1 *)_this)->_pid));
		/* merge: .(goto)(0, 18, 17) */
		reached[1][18] = 1;
		;
		_m = 3; goto P999; /* 12 */
	case 21: // STATE 15 - modelo_final.pml:127 - [(((produced>=15)&&(buffer_count==0)))] (0:0:0 - 1)
		IfNotBlocked
		reached[1][15] = 1;
		if (!(((((int)now.produced)>=15)&&(((int)now.buffer_count)==0))))
			continue;
		_m = 3; goto P999; /* 0 */
	case 22: // STATE 20 - modelo_final.pml:130 - [-end-] (0:0:0 - 3)
		IfNotBlocked
		reached[1][20] = 1;
		if (!delproc(1, II)) continue;
		_m = 3; goto P999; /* 0 */

		 /* PROC Ingestor */
	case 23: // STATE 1 - modelo_final.pml:50 - [((((buffer_count<5)&&(produced<15))&&!(mutex)))] (13:0:6 - 1)
		IfNotBlocked
		reached[0][1] = 1;
		if (!((((((int)now.buffer_count)<5)&&(((int)now.produced)<15))&& !(((int)now.mutex)))))
			continue;
		/* merge: mutex = 1(13, 2, 13) */
		reached[0][2] = 1;
		(trpt+1)->bup.ovals = grab_ints(6);
		(trpt+1)->bup.ovals[0] = ((int)now.mutex);
		now.mutex = 1;
#ifdef VAR_RANGES
		logval("mutex", ((int)now.mutex));
#endif
		;
		/* merge: assert(!(in_critical))(13, 3, 13) */
		reached[0][3] = 1;
		spin_assert( !(((int)now.in_critical)), " !(in_critical)", II, tt, t);
		/* merge: in_critical = 1(13, 4, 13) */
		reached[0][4] = 1;
		(trpt+1)->bup.ovals[1] = ((int)now.in_critical);
		now.in_critical = 1;
#ifdef VAR_RANGES
		logval("in_critical", ((int)now.in_critical));
#endif
		;
		/* merge: buffer_count = (buffer_count+1)(13, 5, 13) */
		reached[0][5] = 1;
		(trpt+1)->bup.ovals[2] = ((int)now.buffer_count);
		now.buffer_count = (((int)now.buffer_count)+1);
#ifdef VAR_RANGES
		logval("buffer_count", ((int)now.buffer_count));
#endif
		;
		/* merge: produced = (produced+1)(13, 6, 13) */
		reached[0][6] = 1;
		(trpt+1)->bup.ovals[3] = ((int)now.produced);
		now.produced = (((int)now.produced)+1);
#ifdef VAR_RANGES
		logval("produced", ((int)now.produced));
#endif
		;
		/* merge: printf('Ingestor: Expediente cargado. Buffer=%d | Total=%d\\n',buffer_count,produced)(13, 7, 13) */
		reached[0][7] = 1;
		Printf("Ingestor: Expediente cargado. Buffer=%d | Total=%d\n", ((int)now.buffer_count), ((int)now.produced));
		/* merge: in_critical = 0(13, 8, 13) */
		reached[0][8] = 1;
		(trpt+1)->bup.ovals[4] = ((int)now.in_critical);
		now.in_critical = 0;
#ifdef VAR_RANGES
		logval("in_critical", ((int)now.in_critical));
#endif
		;
		/* merge: mutex = 0(13, 9, 13) */
		reached[0][9] = 1;
		(trpt+1)->bup.ovals[5] = ((int)now.mutex);
		now.mutex = 0;
#ifdef VAR_RANGES
		logval("mutex", ((int)now.mutex));
#endif
		;
		/* merge: .(goto)(0, 14, 13) */
		reached[0][14] = 1;
		;
		_m = 3; goto P999; /* 9 */
	case 24: // STATE 11 - modelo_final.pml:72 - [((produced>=15))] (0:0:0 - 1)
		IfNotBlocked
		reached[0][11] = 1;
		if (!((((int)now.produced)>=15)))
			continue;
		_m = 3; goto P999; /* 0 */
	case 25: // STATE 16 - modelo_final.pml:75 - [-end-] (0:0:0 - 3)
		IfNotBlocked
		reached[0][16] = 1;
		if (!delproc(1, II)) continue;
		_m = 3; goto P999; /* 0 */
	case  _T5:	/* np_ */
		if (!((!(trpt->o_pm&4) && !(trpt->tau&128))))
			continue;
		/* else fall through */
	case  _T2:	/* true */
		_m = 3; goto P999;
#undef rand
	}

