import { ISquadra } from 'app/shared/model/squadra.model';

export interface ICoach {
  id?: number;
  nome?: string;
  cognome?: string | null;
  itsMe?: boolean | null;
  squadra?: ISquadra | null;
}

export const defaultValue: Readonly<ICoach> = {
  itsMe: false,
};
