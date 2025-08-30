import { ILega } from 'app/shared/model/lega.model';

export interface ISquadra {
  id?: number;
  nome?: string;
  lega?: ILega | null;
}

export const defaultValue: Readonly<ISquadra> = {};
