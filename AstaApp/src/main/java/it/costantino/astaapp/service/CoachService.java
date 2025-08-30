package it.costantino.astaapp.service;

import it.costantino.astaapp.service.dto.CoachDTO;
import java.util.List;
import java.util.Optional;

/**
 * Service Interface for managing {@link it.costantino.astaapp.domain.Coach}.
 */
public interface CoachService {
    /**
     * Save a coach.
     *
     * @param coachDTO the entity to save.
     * @return the persisted entity.
     */
    CoachDTO save(CoachDTO coachDTO);

    /**
     * Updates a coach.
     *
     * @param coachDTO the entity to update.
     * @return the persisted entity.
     */
    CoachDTO update(CoachDTO coachDTO);

    /**
     * Partially updates a coach.
     *
     * @param coachDTO the entity to update partially.
     * @return the persisted entity.
     */
    Optional<CoachDTO> partialUpdate(CoachDTO coachDTO);

    /**
     * Get all the coaches.
     *
     * @return the list of entities.
     */
    List<CoachDTO> findAll();

    /**
     * Get the "id" coach.
     *
     * @param id the id of the entity.
     * @return the entity.
     */
    Optional<CoachDTO> findOne(Long id);

    /**
     * Delete the "id" coach.
     *
     * @param id the id of the entity.
     */
    void delete(Long id);
}
