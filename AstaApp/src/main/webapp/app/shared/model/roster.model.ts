import { ISquadra } from 'app/shared/model/squadra.model';

export interface IRoster {
  id?: number;
  full?: boolean;
  port?: number;
  dif?: number;
  cc?: number;
  att?: number;
  squadra?: ISquadra | null;
}

export const defaultValue: Readonly<IRoster> = {
  full: false,
};
