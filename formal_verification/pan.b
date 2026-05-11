	switch (t->back) {
	default: Uerror("bad return move");
	case  0: goto R999; /* nothing to undo */

		 /* CLAIM safety_buffer */
;
		
	case 3: // STATE 1
		goto R999;

	case 4: // STATE 10
		;
		p_restor(II);
		;
		;
		goto R999;

		 /* CLAIM liveness_property */
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

		 /* PROC Worker */
;
		;
		;
		;
		
	case 10: // STATE 8
		;
		now.mutex = trpt->bup.ovals[3];
		now.processed_items = trpt->bup.ovals[2];
		now.buffer_count = trpt->bup.ovals[1];
		now.mutex = trpt->bup.ovals[0];
		;
		ungrab_ints(trpt->bup.ovals, 4);
		goto R999;
;
		;
		
	case 12: // STATE 15
		;
		p_restor(II);
		;
		;
		goto R999;

		 /* PROC Ingestor */
;
		;
		;
		;
		
	case 15: // STATE 4
		;
		((P0 *)_this)->items_produced = trpt->bup.ovals[1];
		now.buffer_count = trpt->bup.ovals[0];
		;
		ungrab_ints(trpt->bup.ovals, 2);
		goto R999;

	case 16: // STATE 7
		;
	/* 0 */	((P0 *)_this)->items_produced = trpt->bup.oval;
		;
		;
		goto R999;

	case 17: // STATE 13
		;
		p_restor(II);
		;
		;
		goto R999;
	}

