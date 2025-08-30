package it.costantino.astaapp.repository;

import it.costantino.astaapp.domain.Squadra;
import org.springframework.data.jpa.repository.*;
import org.springframework.stereotype.Repository;

/**
 * Spring Data JPA repository for the Squadra entity.
 */
@SuppressWarnings("unused")
@Repository
public interface SquadraRepository extends JpaRepository<Squadra, Long> {}
