package it.costantino.astaapp.service;

import it.costantino.astaapp.service.dto.WatchListDTO;
import java.util.List;
import java.util.Optional;

/**
 * Service Interface for managing {@link it.costantino.astaapp.domain.WatchList}.
 */
public interface WatchListService {
    /**
     * Save a watchList.
     *
     * @param watchListDTO the entity to save.
     * @return the persisted entity.
     */
    WatchListDTO save(WatchListDTO watchListDTO);

    /**
     * Updates a watchList.
     *
     * @param watchListDTO the entity to update.
     * @return the persisted entity.
     */
    WatchListDTO update(WatchListDTO watchListDTO);

    /**
     * Partially updates a watchList.
     *
     * @param watchListDTO the entity to update partially.
     * @return the persisted entity.
     */
    Optional<WatchListDTO> partialUpdate(WatchListDTO watchListDTO);

    /**
     * Get all the watchLists.
     *
     * @return the list of entities.
     */
    List<WatchListDTO> findAll();

    /**
     * Get the "id" watchList.
     *
     * @param id the id of the entity.
     * @return the entity.
     */
    Optional<WatchListDTO> findOne(Long id);

    /**
     * Delete the "id" watchList.
     *
     * @param id the id of the entity.
     */
    void delete(Long id);
}
