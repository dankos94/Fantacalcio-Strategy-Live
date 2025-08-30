package it.costantino.astaapp.service;

import it.costantino.astaapp.service.dto.GiocatoreDTO;
import java.util.List;
import java.util.Optional;

/**
 * Service Interface for managing {@link it.costantino.astaapp.domain.Giocatore}.
 */
public interface GiocatoreService {
    /**
     * Save a giocatore.
     *
     * @param giocatoreDTO the entity to save.
     * @return the persisted entity.
     */
    GiocatoreDTO save(GiocatoreDTO giocatoreDTO);

    /**
     * Updates a giocatore.
     *
     * @param giocatoreDTO the entity to update.
     * @return the persisted entity.
     */
    GiocatoreDTO update(GiocatoreDTO giocatoreDTO);

    /**
     * Partially updates a giocatore.
     *
     * @param giocatoreDTO the entity to update partially.
     * @return the persisted entity.
     */
    Optional<GiocatoreDTO> partialUpdate(GiocatoreDTO giocatoreDTO);

    /**
     * Get all the giocatores.
     *
     * @return the list of entities.
     */
    List<GiocatoreDTO> findAll();

    /**
     * Get the "id" giocatore.
     *
     * @param id the id of the entity.
     * @return the entity.
     */
    Optional<GiocatoreDTO> findOne(Long id);

    /**
     * Delete the "id" giocatore.
     *
     * @param id the id of the entity.
     */
    void delete(Long id);
}
