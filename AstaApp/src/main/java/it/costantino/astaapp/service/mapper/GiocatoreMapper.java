package it.costantino.astaapp.service.mapper;

import it.costantino.astaapp.domain.Giocatore;
import it.costantino.astaapp.domain.Squadra;
import it.costantino.astaapp.domain.WatchList;
import it.costantino.astaapp.service.dto.GiocatoreDTO;
import it.costantino.astaapp.service.dto.SquadraDTO;
import it.costantino.astaapp.service.dto.WatchListDTO;
import org.mapstruct.*;

/**
 * Mapper for the entity {@link Giocatore} and its DTO {@link GiocatoreDTO}.
 */
@Mapper(componentModel = "spring")
public interface GiocatoreMapper extends EntityMapper<GiocatoreDTO, Giocatore> {
    @Mapping(target = "squadra", source = "squadra", qualifiedByName = "squadraId")
    @Mapping(target = "watchList", source = "watchList", qualifiedByName = "watchListId")
    GiocatoreDTO toDto(Giocatore s);

    @Named("squadraId")
    @BeanMapping(ignoreByDefault = true)
    @Mapping(target = "id", source = "id")
    SquadraDTO toDtoSquadraId(Squadra squadra);

    @Named("watchListId")
    @BeanMapping(ignoreByDefault = true)
    @Mapping(target = "id", source = "id")
    WatchListDTO toDtoWatchListId(WatchList watchList);
}
