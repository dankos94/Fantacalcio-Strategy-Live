package it.costantino.astaapp.repository;

import it.costantino.astaapp.domain.Giocatore;
import org.springframework.data.jpa.repository.*;
import org.springframework.stereotype.Repository;

/**
 * Spring Data JPA repository for the Giocatore entity.
 */
@SuppressWarnings("unused")
@Repository
public interface GiocatoreRepository extends JpaRepository<Giocatore, Long> {}
