import stagione from 'app/entities/stagione/stagione.reducer';
import lega from 'app/entities/lega/lega.reducer';
import squadra from 'app/entities/squadra/squadra.reducer';
import coach from 'app/entities/coach/coach.reducer';
import giocatore from 'app/entities/giocatore/giocatore.reducer';
import roster from 'app/entities/roster/roster.reducer';
import watchList from 'app/entities/watch-list/watch-list.reducer';
/* jhipster-needle-add-reducer-import - JHipster will add reducer here */

const entitiesReducers = {
  stagione,
  lega,
  squadra,
  coach,
  giocatore,
  roster,
  watchList,
  /* jhipster-needle-add-reducer-combine - JHipster will add reducer here */
};

export default entitiesReducers;
