package it.costantino.astaapp.service;

import it.costantino.astaapp.service.dto.RosterDTO;
import java.util.List;
import java.util.Optional;

/**
 * Service Interface for managing {@link it.costantino.astaapp.domain.Roster}.
 */
public interface RosterService {
    /**
     * Save a roster.
     *
     * @param rosterDTO the entity to save.
     * @return the persisted entity.
     */
    RosterDTO save(RosterDTO rosterDTO);

    /**
     * Updates a roster.
     *
     * @param rosterDTO the entity to update.
     * @return the persisted entity.
     */
    RosterDTO update(RosterDTO rosterDTO);

    /**
     * Partially updates a roster.
     *
     * @param rosterDTO the entity to update partially.
     * @return the persisted entity.
     */
    Optional<RosterDTO> partialUpdate(RosterDTO rosterDTO);

    /**
     * Get all the rosters.
     *
     * @return the list of entities.
     */
    List<RosterDTO> findAll();

    /**
     * Get the "id" roster.
     *
     * @param id the id of the entity.
     * @return the entity.
     */
    Optional<RosterDTO> findOne(Long id);

    /**
     * Delete the "id" roster.
     *
     * @param id the id of the entity.
     */
    void delete(Long id);
}
