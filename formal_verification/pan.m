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

		 /* CLAIM safety_buffer */
	case 3: // STATE 1 - _spin_nvr.tmp:14 - [(!((buffer_count<=5)))] (6:0:0 - 1)
		
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
		if (!( !((((int)now.buffer_count)<=5))))
			continue;
		/* merge: assert(!(!((buffer_count<=5))))(0, 2, 6) */
		reached[3][2] = 1;
		spin_assert( !( !((((int)now.buffer_count)<=5))), " !( !((buffer_count<=5)))", II, tt, t);
		/* merge: .(goto)(0, 7, 6) */
		reached[3][7] = 1;
		;
		_m = 3; goto P999; /* 2 */
	case 4: // STATE 10 - _spin_nvr.tmp:19 - [-end-] (0:0:0 - 1)
		
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
		reached[3][10] = 1;
		if (!delproc(1, II)) continue;
		_m = 3; goto P999; /* 0 */

		 /* CLAIM liveness_property */
	case 5: // STATE 1 - _spin_nvr.tmp:3 - [((!(!((processed_items<15)))&&!((processed_items==15))))] (0:0:0 - 1)
		
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
		if (!(( !( !((((int)now.processed_items)<15)))&& !((((int)now.processed_items)==15)))))
			continue;
		_m = 3; goto P999; /* 0 */
	case 6: // STATE 8 - _spin_nvr.tmp:8 - [(!((processed_items==15)))] (0:0:0 - 1)
		
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
		reached[2][8] = 1;
		if (!( !((((int)now.processed_items)==15))))
			continue;
		_m = 3; goto P999; /* 0 */
	case 7: // STATE 13 - _spin_nvr.tmp:10 - [-end-] (0:0:0 - 1)
		
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
		reached[2][13] = 1;
		if (!delproc(1, II)) continue;
		_m = 3; goto P999; /* 0 */

		 /* PROC Worker */
	case 8: // STATE 1 - model_tp.pml:40 - [((processed_items<15))] (0:0:0 - 1)
		IfNotBlocked
		reached[1][1] = 1;
		if (!((((int)now.processed_items)<15)))
			continue;
		_m = 3; goto P999; /* 0 */
	case 9: // STATE 2 - model_tp.pml:42 - [((buffer_count>0))] (0:0:0 - 1)
		IfNotBlocked
		reached[1][2] = 1;
		if (!((((int)now.buffer_count)>0)))
			continue;
		_m = 3; goto P999; /* 0 */
	case 10: // STATE 3 - model_tp.pml:46 - [(!(mutex))] (12:0:4 - 1)
		IfNotBlocked
		reached[1][3] = 1;
		if (!( !(((int)now.mutex))))
			continue;
		/* merge: mutex = 1(12, 4, 12) */
		reached[1][4] = 1;
		(trpt+1)->bup.ovals = grab_ints(4);
		(trpt+1)->bup.ovals[0] = ((int)now.mutex);
		now.mutex = 1;
#ifdef VAR_RANGES
		logval("mutex", ((int)now.mutex));
#endif
		;
		/* merge: buffer_count = (buffer_count-1)(12, 5, 12) */
		reached[1][5] = 1;
		(trpt+1)->bup.ovals[1] = ((int)now.buffer_count);
		now.buffer_count = (((int)now.buffer_count)-1);
#ifdef VAR_RANGES
		logval("buffer_count", ((int)now.buffer_count));
#endif
		;
		/* merge: processed_items = (processed_items+1)(12, 6, 12) */
		reached[1][6] = 1;
		(trpt+1)->bup.ovals[2] = ((int)now.processed_items);
		now.processed_items = (((int)now.processed_items)+1);
#ifdef VAR_RANGES
		logval("processed_items", ((int)now.processed_items));
#endif
		;
		/* merge: printf('WORKER %d: Procesando. Total procesado: %d\\n',_pid,processed_items)(12, 7, 12) */
		reached[1][7] = 1;
		Printf("WORKER %d: Procesando. Total procesado: %d\n", ((int)((P1 *)_this)->_pid), ((int)now.processed_items));
		/* merge: mutex = 0(12, 8, 12) */
		reached[1][8] = 1;
		(trpt+1)->bup.ovals[3] = ((int)now.mutex);
		now.mutex = 0;
#ifdef VAR_RANGES
		logval("mutex", ((int)now.mutex));
#endif
		;
		/* merge: .(goto)(0, 13, 12) */
		reached[1][13] = 1;
		;
		_m = 3; goto P999; /* 6 */
	case 11: // STATE 10 - model_tp.pml:55 - [((processed_items==15))] (0:0:0 - 1)
		IfNotBlocked
		reached[1][10] = 1;
		if (!((((int)now.processed_items)==15)))
			continue;
		_m = 3; goto P999; /* 0 */
	case 12: // STATE 15 - model_tp.pml:58 - [-end-] (0:0:0 - 3)
		IfNotBlocked
		reached[1][15] = 1;
		if (!delproc(1, II)) continue;
		_m = 3; goto P999; /* 0 */

		 /* PROC Ingestor */
	case 13: // STATE 1 - model_tp.pml:22 - [((items_produced<15))] (0:0:0 - 1)
		IfNotBlocked
		reached[0][1] = 1;
		if (!((((int)((P0 *)_this)->items_produced)<15)))
			continue;
		_m = 3; goto P999; /* 0 */
	case 14: // STATE 2 - model_tp.pml:24 - [((buffer_count<5))] (0:0:0 - 1)
		IfNotBlocked
		reached[0][2] = 1;
		if (!((((int)now.buffer_count)<5)))
			continue;
		_m = 3; goto P999; /* 0 */
	case 15: // STATE 3 - model_tp.pml:27 - [buffer_count = (buffer_count+1)] (0:10:2 - 1)
		IfNotBlocked
		reached[0][3] = 1;
		(trpt+1)->bup.ovals = grab_ints(2);
		(trpt+1)->bup.ovals[0] = ((int)now.buffer_count);
		now.buffer_count = (((int)now.buffer_count)+1);
#ifdef VAR_RANGES
		logval("buffer_count", ((int)now.buffer_count));
#endif
		;
		/* merge: items_produced = (items_produced+1)(10, 4, 10) */
		reached[0][4] = 1;
		(trpt+1)->bup.ovals[1] = ((int)((P0 *)_this)->items_produced);
		((P0 *)_this)->items_produced = (((int)((P0 *)_this)->items_produced)+1);
#ifdef VAR_RANGES
		logval("Ingestor:items_produced", ((int)((P0 *)_this)->items_produced));
#endif
		;
		/* merge: printf('INGESTOR: Enviado item %d. Buffer actual: %d\\n',items_produced,buffer_count)(10, 5, 10) */
		reached[0][5] = 1;
		Printf("INGESTOR: Enviado item %d. Buffer actual: %d\n", ((int)((P0 *)_this)->items_produced), ((int)now.buffer_count));
		/* merge: .(goto)(0, 11, 10) */
		reached[0][11] = 1;
		;
		_m = 3; goto P999; /* 3 */
	case 16: // STATE 7 - model_tp.pml:31 - [((items_produced==15))] (13:0:1 - 1)
		IfNotBlocked
		reached[0][7] = 1;
		if (!((((int)((P0 *)_this)->items_produced)==15)))
			continue;
		if (TstOnly) return 1; /* TT */
		/* dead 1: items_produced */  (trpt+1)->bup.oval = ((P0 *)_this)->items_produced;
#ifdef HAS_CODE
		if (!readtrail)
#endif
			((P0 *)_this)->items_produced = 0;
		/* merge: printf('INGESTOR: Finalizado. No más registros.\\n')(0, 8, 13) */
		reached[0][8] = 1;
		Printf("INGESTOR: Finalizado. No más registros.\n");
		/* merge: goto :b0(0, 9, 13) */
		reached[0][9] = 1;
		;
		_m = 3; goto P999; /* 2 */
	case 17: // STATE 13 - model_tp.pml:35 - [-end-] (0:0:0 - 3)
		IfNotBlocked
		reached[0][13] = 1;
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

