package it.costantino.astaapp.service.mapper;

import it.costantino.astaapp.domain.Squadra;
import it.costantino.astaapp.domain.WatchList;
import it.costantino.astaapp.service.dto.SquadraDTO;
import it.costantino.astaapp.service.dto.WatchListDTO;
import org.mapstruct.*;

/**
 * Mapper for the entity {@link WatchList} and its DTO {@link WatchListDTO}.
 */
@Mapper(componentModel = "spring")
public interface WatchListMapper extends EntityMapper<WatchListDTO, WatchList> {
    @Mapping(target = "squadra", source = "squadra", qualifiedByName = "squadraId")
    WatchListDTO toDto(WatchList s);

    @Named("squadraId")
    @BeanMapping(ignoreByDefault = true)
    @Mapping(target = "id", source = "id")
    SquadraDTO toDtoSquadraId(Squadra squadra);
}
