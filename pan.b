	switch (t->back) {
	default: Uerror("bad return move");
	case  0: goto R999; /* nothing to undo */

		 /* CLAIM buffer_seguro */
;
		
	case 3: // STATE 1
		goto R999;

	case 4: // STATE 10
		;
		p_restor(II);
		;
		;
		goto R999;

		 /* CLAIM anti_starvation_worker2 */
;
		;
		;
		;
		
	case 7: // STATE 13
		;
		p_restor(II);
		;
		;
		goto R999;

		 /* CLAIM anti_starvation_worker1 */
;
		;
		;
		;
		
	case 10: // STATE 13
		;
		p_restor(II);
		;
		;
		goto R999;

		 /* CLAIM anti_starvation_worker0 */
;
		;
		;
		;
		
	case 13: // STATE 13
		;
		p_restor(II);
		;
		;
		goto R999;

		 /* CLAIM progreso */
;
		;
		;
		;
		
	case 16: // STATE 13
		;
		p_restor(II);
		;
		;
		goto R999;

		 /* CLAIM exclusion_mutua */
;
		
	case 17: // STATE 1
		goto R999;

	case 18: // STATE 10
		;
		p_restor(II);
		;
		;
		goto R999;

		 /* PROC Limpiador */

	case 19: // STATE 1
		;
		((P1 *)_this)->id = trpt->bup.oval;
		;
		goto R999;

	case 20: // STATE 12
		;
		now.mutex = trpt->bup.ovals[7];
		now.in_critical = trpt->bup.ovals[6];
		now.worker_active[ Index(((P1 *)_this)->id, 3) ] = trpt->bup.ovals[5];
		now.worker_jobs[ Index(((P1 *)_this)->id, 3) ] = trpt->bup.ovals[4];
		now.consumed = trpt->bup.ovals[3];
		now.buffer_count = trpt->bup.ovals[2];
		now.in_critical = trpt->bup.ovals[1];
		now.mutex = trpt->bup.ovals[0];
		;
		ungrab_ints(trpt->bup.ovals, 8);
		goto R999;
;
		;
		
	case 22: // STATE 20
		;
		p_restor(II);
		;
		;
		goto R999;

		 /* PROC Ingestor */

	case 23: // STATE 9
		;
		now.mutex = trpt->bup.ovals[5];
		now.in_critical = trpt->bup.ovals[4];
		now.produced = trpt->bup.ovals[3];
		now.buffer_count = trpt->bup.ovals[2];
		now.in_critical = trpt->bup.ovals[1];
		now.mutex = trpt->bup.ovals[0];
		;
		ungrab_ints(trpt->bup.ovals, 6);
		goto R999;
;
		;
		
	case 25: // STATE 16
		;
		p_restor(II);
		;
		;
		goto R999;
	}

